#!/usr/bin/env python3

import argparse
import sys
import os
import subprocess
import logging
import math
import urllib.parse
from threading import Timer
from MythTV import Job, Recorded, MythError, MythDB
from MythTV.utility import datetime
from MythTV.services_api import send as api

sys.path.append("/usr/bin")


class Status:
    mythJob = None
    mythJobId = 0

    def __init__(self, jobId=0):
        if jobId and not Status.mythJob:
            Status.mythJobId = jobId
            Status.mythJob = Job(jobId)
            Status.mythJob.update(status=Job.STARTING)
            self.setComment('Starting job...')

    def logError(self, errorMsg):
        logging.error(errorMsg)
        self.setComment(errorMsg)
        self.setStatus(Job.ERRORED)
        

    def setComment(self, msg):
        logging.info(msg)
        if Status.mythJob:
            Status.mythJob.setComment(msg)
    
    def setProgress(self, progress, eta):
        if Status.mythJob:
            Status.mythJob.setComment('Progress: {} %\nRemaining time: {}'.format(progress, eta))

    def setStatus(self, newStatus):
        logging.debug('Setting job status to {}'.format(newStatus))
        if Status.mythJob:
            Status.mythJob.setStatus(newStatus)

    def getCmd(self):
        if Status.mythJobId == 0:
            return Job.UNKNOWN
        # create new job object to pull current state from database
        return Job(Status.mythJobId).cmds

    def getChanId(self):
        if Status.mythJob:
            return Status.mythJob.chanid
        return None

    def getStartTime(self):
        if Status.mythJob:
            return Status.mythJob.starttime
        return None

    def showNotification(self, msgText, msgType):
        args = []
        args.append('mythutil')
        args.append('--notification')
        args.append('--origin')
        args.append('\"' + __file__ + '\"')
        args.append('--timeout')
        args.append('60')
        args.append('--message_text')
        args.append(msgText)
        args.append('--type')
        args.append(msgType)
        cp = subprocess.run(args, capture_output=True, text=True)
        if cp.returncode != 0:
            logging.error(cp.stderr)
        if msgType == 'error':
            logging.error(msgText)
        elif msgType == "warning":
            logging.warning(msgText)
        elif msgType == "normal":
            logging.info(msgText)

class VideoFilePath:
    def __init__(self):
        self.title = None
        self.subtitle = None
        self.season = 0
        self.episode = 0

    def build(self):
        dirName = self.__buildDir()
        if not dirName:
            return None
        fileName = self.__buildName()
        return os.path.join(dirName, fileName)

    # Uses the following criteria by ascending priority
    # 1. Storage dir with maximum free space
    # 2. Directory matching recording title (useful for series)
    # 3. Directory containing files matching the title
    def __buildDir(self):
        db = MythDB()
        matchDirName = None
        title = "_".join(self.title.split())
        maxFreeSpace = 0
        maxFreeDirName = None
        for sg in db.getStorageGroup(groupname='Videos'):
            # search given group
            if sg.local and os.path.isdir(sg.dirname):
                # get avaliable space of storage group partition
                # and use storage group with max. available space
                freeSpace = self.__getFreeSpace(sg.dirname)
                logging.debug('Storage group {} -> space {}'.format(sg.dirname, freeSpace))
                if freeSpace > maxFreeSpace:
                    maxFreeDirName = sg.dirname
                    maxFreeSpace = freeSpace
                for root, dirs, files in os.walk(sg.dirname, followlinks=True):
                    # first check subdir for match
                    for d in dirs:
                        if self.__matchTitle(title, d):
                            matchDirName = os.path.join(root, d)
                    # check file names for match
                    for f in files:
                        if self.__matchTitle(title, f):
                            logging.debug('Using storage dir with files matching title')
                            return root
        # return directory matching title if found
        if matchDirName:
            logging.debug('Using storage dir matching title')
            return matchDirName
        # return storage directory with max free space
        logging.debug('Using storage dir with max. space')
        return maxFreeDirName

    def __buildName(self):
        # build output file name: "The_title(_-_|_SxxEyy_][The_Subtitle].m4v"
        parts = []
        if self.title and self.title != "":
            parts.append(self.title)
        if self.season > 0 and self.episode > 0:
            parts.append("S{:0>2}E{:0>2}".format(self.season, self.episode))
        elif self.subtitle and self.subtitle != "":
            parts.append('-')
        if self.subtitle and self.subtitle != "":
            parts.append(self.subtitle)
        return "_".join(' '.join(parts).split()) + ".m4v"

    def __getFreeSpace(self, filename):
        stats = os.statvfs(filename)
        return stats.f_bfree * stats.f_frsize

    # find storage directory by recording title
    def __matchTitle(self, title, name):
        t = title.lower()
        n = name.lower()
        for c in (' ', '_', '-'):
            n = n.replace(c, '')
            t = t.replace(c, '')
        return n.startswith(t)


class Transcoder:
    def __init__(self):
        self.status = Status()
        self.timer = None

    def __abortTranscode(self, process):
        self.status.logError('Aborting transcode due to timeout')
        process.kill()

    # start timer to abort transcode process if it hangs
    def __startTimer(self, timeout, cp):
        self.__stopTimer()
        self.timer = Timer(timeout, self.__abortTranscode, [cp])
        self.timer.start()

    def __stopTimer(self):
        if self.timer is not None:
            self.timer.cancel()
        self.timer = None

    def transcode(self, srcFile, dstFile, preset, timeout):
        # obtain cutlist
        dt = self.status.getStartTime()
        chanid = self.status.getChanId()
        if not dt or not chanid:
            logging.debug('Determine chanid and starttime from filename')
            # extract chanid and starttime from recording file name
            srcFileBaseName,srcFileExt = os.path.splitext(os.path.basename(srcFile))
            (chanid, startTime) = srcFileBaseName.split('_', 2)
            dt = datetime.duck(startTime)

        # convert starttime from UTC
        dt = datetime.fromnaiveutc(dt)
        logging.debug('Using chanid={} and startime={}'.format(chanid, dt))

        try:
            db = MythDB()
            rec = Recorded((chanid, dt), db)
            cuts = rec.markup.getuncutlist()
        except MythError as err:
            logging.error('Could not read cutlist ({})'.format(err.message))
            return 1

        if len(cuts):
            logging.debug('Found {} cuts: {}'.format(len(cuts), cuts))


        if len(cuts) == 0:
            # transcode whole file directly
            res = self.__transcodePart(srcFile, dstFile, preset, timeout)
        if len(cuts) == 1:
            # transcode single part directly
            res = self.__transcodePart(srcFile, dstFile, preset, timeout, cuts[0])
        else:
            # transcode each part on its own
            cutNumber = 1
            tmpFiles = []
            dstFileBaseName,dstFileExt = os.path.splitext(dstFile)
            for cut in cuts:
                partDstFile = '{}_part_{}{}'.format(dstFileBaseName, cutNumber, dstFileExt)
                logging.info('Transcoding part {}/{} to {}'.format(cutNumber, len(cuts), partDstFile))
                res = self.__transcodePart(srcFile, partDstFile, preset, timeout, cut)
                if res != 0:
                    break
                cutNumber += 1
                tmpFiles.append(partDstFile)

            # merge transcoded parts
            if len(cuts) == len(tmpFiles):
                logging.debug('Merging transcoded parts {}'.format(tmpFiles))
                listFile = '{}_partlist.txt'.format(dstFileBaseName)
                with open(listFile, "w") as textFile:
                    for tmpFile in tmpFiles:
                        textFile.write('file {}\n'.format(tmpFile))

                tmpFiles.append(listFile)
                self.status.setComment('Merging transcoded parts')

                args = []
                args.append('ffmpeg')
                args.append('-f')
                args.append('concat')
                args.append('-safe')
                args.append('0')
                args.append('-i')
                args.append(listFile)
                args.append('-c')
                args.append('copy')
                args.append(dstFile)
                logging.debug('Executing {}'.format(args))
                cp = subprocess.run(args, capture_output=True, text=True)
                res = cp.returncode
                if res != 0:
                    logging.error(cp.stderr)
                    tmpFiles.append(dstFile)

            # delete transcoded parts
            for tmpFile in tmpFiles:
                if os.path.isfile(tmpFile):
                    os.remove(tmpFile)

        if res == 0:
            # rescan videos
            self.__addVideo(srcFile, dstFile)
            self.__scanVideos()

        return res

    def __transcodePart(self, srcFile, dstFile, preset, timeout, frames=None):
        # start the transcoding process
        args = []
        args.append('HandBrakeCLI')
        args.append('--preset')
        args.append(preset)
        args.append('-i')
        args.append(srcFile)
        args.append('-o')
        args.append(dstFile)
        if not frames is None:
            logging.debug('Transcoding from frame {} to {}'.format(frames[0], frames[1]))
            # pass start and end position of remaining part to handbrake
            args.append('--start-at')
            args.append('frame:{}'.format(frames[0]))
            # stop it relative to start position
            args.append('--stop-at')
            args.append('frame:{}'.format(frames[1]-frames[0]))

        logging.debug('Executing {}'.format(args))
        cp = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # start timer to abort transcode process if it hangs
        self.__startTimer(timeout, cp)

        line = ''
        lastProgress = 0
        while True:
            nl = cp.stdout.read(1)
            if nl == '' and cp.poll() is not None:
                break  # Aborted, no characters available, process died.
            if nl == '\n':
                lastToken = ''
                progress = '0'
                eta = None
                # new line, restart abort timer
                self.__startTimer(timeout, cp)
                for token in line.split():
                    if token == '%':
                        progress = lastToken
                    if lastToken == 'ETA':
                        eta = token.replace(')', '')
                    if eta and progress:
                        break
                    lastToken = token
                if eta and int(float(progress)) > lastProgress:
                    self.status.setProgress(progress, eta)
                    lastProgress = int(float(progress))
                    # check if job was stopped externally
                    if self.status.getCmd() == Job.STOP:
                        cp.kill()
                        break
                line = ''
            else:
                line += nl
        res = cp.wait()
        self.__stopTimer()
        # remove video file on failure
        if res != 0:
            # print transcoding error output
            logging.error(cp.stderr.read())
            if os.path.isfile(dstFile):
                os.remove(dstFile)

        return res
        
    def __scanVideos(self):
        self.status.setComment('Triggering video rescan')

        # scan videos
        args = []
        args.append('mythutil')
        args.append('--scanvideos')
        cp = subprocess.run(args, capture_output=True, text=True)
        if cp.returncode != 0:
            logging.error(cp.stderr)

    def __addVideo(self, recpath, vidpath):
        self.status.setComment("Adding video and metadata to database")
        try:
            mbe = api.Send(host='localhost')

            rd = mbe.send(endpoint='Myth/GetHostName')
            hostname = rd['String']

            # find storage group from video path
            rd = mbe.send(endpoint='Myth/GetStorageGroupDirs', rest=f'HostName={hostname}&GroupName=Videos')
            storage_groups = rd['StorageGroupDirList']['StorageGroupDirs']
            vid_file = None
            for sg in storage_groups:
                sg_path = sg['DirName']
                if vidpath.startswith(sg_path):
                    vid_file = vidpath[len(sg_path):]
                    logging.debug(f'Found video in storage group {sg_path} -> {vid_file}')
                    break

            if not vid_file:
                return

            # add video
            data = {'HostName': hostname, 'FileName': vid_file}
            rd = mbe.send(endpoint='Video/AddVideo', postdata=data, opts={'debug': True, 'wrmi': True})
            if rd['bool'] == 'true':
                logging.info('Successfully added video')

            # get video id
            rd = mbe.send(endpoint='Video/GetVideoByFileName', rest=f'FileName={urllib.parse.quote(vid_file)}')
            vid_id = rd['VideoMetadataInfo']['Id']
            logging.debug(f'Got video id {vid_id}')

            # get recording id)
            rd = mbe.send(endpoint='Dvr/RecordedIdForPathname', rest=f'Pathname={urllib.parse.quote(recpath)}')
            rec_id = rd['int'];
            logging.debug(f'Got recording id {rec_id}')

            # get recording metadata
            rd = mbe.send(endpoint='Dvr/GetRecorded', rest=f'RecordedId={rec_id}')

            # collect metadata
            description = rd['Program']['Description']
            director = []
            actors = []
            for member in rd['Program']['Cast']['CastMembers']:
                if member['Role'] == 'director':
                    director.append(member['Name'])
                if member['Role'] == 'actor':
                    actors.append(member['Name'])
            vid_length = self.__getVideoLength(vidpath)

            # update video metadata
            data = {'Id': vid_id}
            if description:
                data['Plot'] = description
            if vid_length >= 1:
                data['Length'] = vid_length
            if len(director):
                data['Director'] = ', '.join(director)
            if len(actors):
                data['Cast'] = ','.join(actors)
            if len(data) > 1:
                rd = mbe.send(endpoint='Video/UpdateVideoMetadata', postdata=data, opts={'debug': True, 'wrmi': True})
                if rd['bool'] == 'true':
                    logging.info('Successfully updated video metadata')
        except RuntimeError as error:
            logging.error('\nFatal error: "{}"'.format(error))

    def __getVideoLength(self, filename):
        args = []
        args.append('ffprobe')
        args.append('-hide_banner')
        args.append('-v')
        args.append('error')
        args.append('-show_entries')
        args.append('format=duration')
        args.append('-of')
        args.append('default=noprint_wrappers=1:nokey=1')
        args.append(filename)
        logging.debug('Executing {}'.format(args))
        cp = subprocess.run(args, capture_output=True, text=True)
        if cp.returncode != 0:
            return 0
        try:
            return int(math.ceil(float(cp.stdout) / 60.0))
        except ValueError:
            return 0

def formatFileSize(num):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(num) < 1000.0:
            return "%3.1f %s" % (num, unit)
        num /= 1000.0
    return "%.1f %s" % (num, 'PB')


def main():
    parser = argparse.ArgumentParser(description='Transcoding recording and move to videos')
    parser.add_argument('-f', '--file', dest='recFile', help='recording file name')
    parser.add_argument('-d', '--dir', dest='recDir', help='recording directory name')
    parser.add_argument('-p', '--path', dest='recPath', help='recording path name')
    parser.add_argument('-t', '--title', dest='recTitle', help='recording title')
    parser.add_argument('-s', '--subtitle', dest='recSubtitle', help='recording subtitle')
    parser.add_argument('-sn', '--season', dest='recSeason', default=0, type=int, help='recording season number')
    parser.add_argument('-en', '--episode', dest='recEpisode', default=0, type=int, help='recording episode number')
    parser.add_argument('-j', '--jobid', dest='jobId', help='mythtv job id')
    parser.add_argument('--preset', dest='preset', default='General/HQ 1080p30 Surround', help='Handbrake transcoding preset')
    parser.add_argument('--timeout', dest='timeout', default=300, type=int, help='timeout in seconds to abort transcoding process')
    parser.add_argument('-l', '--logfile', dest='logFile', default='', help='optional log file location')
    opts = parser.parse_args()

    if opts.logFile:
        logging.basicConfig(filename=opts.logFile, level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

    logging.debug('Command line: {}'.format(opts))

    status = Status(opts.jobId)

    recPath = None
    if opts.recPath:
        recPath = opts.recPath
    elif opts.recDir and opts.recFile:
        recPath = os.path.join(opts.recDir, opts.recFile)
    if not recPath:
        status.logError('Recording path or recording directoy + recording file not specified')
        sys.exit(1)
    if not os.path.isfile(recPath):
        status.logError('Input recording file does not exist')
        sys.exit(1)

    if opts.recTitle is None and opts.recSubtitle is None:
        status.logError('Title and/or subtitle not specified')
        sys.exit(1)

    # build output file path
    pathBuilder = VideoFilePath()
    pathBuilder.title = opts.recTitle
    pathBuilder.subtitle = opts.recSubtitle
    pathBuilder.season = opts.recSeason
    pathBuilder.episode = opts.recEpisode
    vidPath = pathBuilder.build()
    if not vidPath:
        status.logError('Could not find video storage directory')
        sys.exit(2)
    if os.path.isfile(vidPath):
        status.logError('Output video file already exists: \"{}\"'.format(vidPath))
        sys.exit(3)

    status.setStatus(Job.RUNNING)

    # start transcoding
    logging.info('Started transcoding \"{}\"'.format(opts.recTitle))
    logging.info('Source recording file : {}'.format(recPath))
    logging.info('Destination video file: {}'.format(vidPath))
    res = Transcoder().transcode(recPath, vidPath, opts.preset, opts.timeout)
    if status.getCmd() == Job.STOP:
        status.setStatus(Job.CANCELLED)
        status.setComment('Stopped transcoding')
        status.showNotification('Stopped transcoding \"{}\"'.format(opts.recTitle), 'warning')
        sys.exit(4)
    elif res != 0:
        status.logError('Failed transcoding (error {})'.format(res))
        status.showNotification('Failed transcoding \"{}\" (error {})'.format(opts.recTitle, res), 'error')
        sys.exit(res)

    recSize = os.stat(recPath).st_size
    vidSize = os.stat(vidPath).st_size
    sizeStatus = formatFileSize(recSize) + ' => ' + formatFileSize(vidSize)
    status.showNotification('Finished transcoding \"{}\"'.format(opts.recTitle) + '\n' + sizeStatus, 'normal')
    status.setComment('Finished transcoding\n' + sizeStatus)
    status.setStatus(Job.FINISHED)

    # .. the end
    sys.exit(0)


if __name__ == "__main__":
    main()
