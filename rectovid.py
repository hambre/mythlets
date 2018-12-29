#!/usr/bin/env python2

import argparse
import sys
import os
import subprocess
from threading import Timer
from MythTV import Job
from MythTV.database import DBCache

sys.path.append("/usr/bin")


class Status:
    mythJob = None

    def __init__(self, jobId=0):
        if jobId and not Status.mythJob:
            Status.mythJob = Job(jobId)
            Status.mythJob.update(status=Job.STARTING)
            self.setComment('Starting job...')

    def logError(self, errorMsg):
        self.setComment(errorMsg)
        sys.stderr.write(errorMsg + '\n')
        self.setStatus(Job.ERRORED)

    def setComment(self, msg):
        if Status.mythJob:
            Status.mythJob.setComment(msg)

    def setStatus(self, newStatus):
        if Status.mythJob:
            Status.mythJob.setStatus(newStatus)

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
        res = os.spawnvp(os.P_WAIT, 'mythutil', args)
        if msgType == 'error':
            sys.stderr.write(msgText + '\n')


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

    # Uses the following criteria by descending priority
    # 1. Storage dir with maximum free space
    # 2. Directory matching recording title (useful for series)
    # 3. Directory containing files matching the title
    def __buildDir(self):
        db = DBCache(None)
        matchDirName = None
        title = self.__decodeName("_".join(self.title.split()))
        maxFreeSpace = 0
        maxFreeDirName = None
        for sg in db.getStorageGroup(groupname='Videos'):
            # search given group
            if sg.local and os.path.isdir(sg.dirname):
                # get avaliable space of storage group partition
                # and use storage group with max. available space
                freeSpace = self.__getFreeSpace(sg.dirname)
                if freeSpace > maxFreeSpace:
                    maxFreeDirName = sg.dirname
                    maxFreeSpace = freeSpace
                for root, dirs, files in os.walk(sg.dirname, followlinks=True):
                    # first check subdir for match
                    for d in dirs:
                        if self.__matchTitle(title, self.__decodeName(d)):
                            matchDirName = os.path.join(root, d)
                    # check file names for match
                    for f in files:
                        if self.__matchTitle(title, self.__decodeName(f)):
                            return root
        # return directory matching title if found
        if matchDirName:
            return matchDirName
        # return storage directory with max free space
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
        return self.__decodeName("_".join(' '.join(parts).split()) + ".m4v")

    def __getFreeSpace(self, filename):
        stats = os.statvfs(filename)
        return stats.f_bfree * stats.f_frsize

    def __decodeName(self, name):
        if type(name) == str:  # leave unicode ones alone
            try:
                name = name.decode('utf8')
            except UnicodeDecodeError:
                name = name.decode('windows-1252')
        return name

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
        # start the transcoding process
        args = []
        args.append('HandBrakeCLI')
        args.append('--preset')
        args.append(preset)
        args.append('-i')
        args.append(srcFile)
        args.append('-o')
        args.append(dstFile)
        cp = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # start timer to abort transcode process if it hangs
        self.__startTimer(timeout, cp)

        line = ''
        lastProgress = 0
        while True:
            nl = cp.stdout.read(1)
            if nl == '' and cp.poll() is not None:
                break  # Aborted, no characters available, process died.
            if nl == '\n':
                line = ''
            elif nl == '\r':
                lastToken = ''
                progress = '0'
                eta = None
                for token in line.decode('utf-8').split():
                    if token == '%':
                        progress = lastToken
                    if lastToken == 'ETA':
                        eta = token.replace(')', '')
                    if eta and progress:
                        break
                    lastToken = token
                if eta and int(float(progress)) > lastProgress:
                    # new progress, restart abort timer
                    self.__startTimer(timeout, cp)
                    self.status.setComment('Progress: {} %\nRemaining time: {}'.format(progress, eta))
                    lastProgress = int(float(progress))
                line = ''
            else:
                line += nl
        res = cp.wait()
        self.__stopTimer()
        # remove video file on failure
        if res != 0:
            if os.path.isfile(dstFile):
                os.remove(dstFile)
            return res

        # rescan videos
        self.__scanVideos()

        return res

    def __scanVideos(self):
        self.status.setComment('Triggering video rescan')

        # scan videos
        args = []
        args.append('mythutil')
        args.append('--scanvideos')
        os.spawnvp(os.P_WAIT, 'mythutil', args)


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
    parser.add_argument('--preset', dest='preset', default='HQ 1080p30 Surround', help='Handbrake transcoding preset')
    parser.add_argument('--timeout', dest='timeout', default=300, type=int, help='timeout in seconds to abort transcoding process')
    opts = parser.parse_args()

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
    res = Transcoder().transcode(recPath, vidPath, opts.preset, opts.timeout)
    if res != 0:
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
