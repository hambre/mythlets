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
    myth_job = None
    myth_job_id = 0

    def __init__(self, job_id=0):
        if job_id and not Status.myth_job:
            Status.myth_job_id = job_id
            Status.myth_job = Job(job_id)
            Status.myth_job.update(status=Job.STARTING)
            self.set_comment('Starting job...')

    def set_error(self, msg):
        logging.error(msg)
        self.set_comment(msg)
        self.set_status(Job.ERRORED)
        

    def set_comment(self, msg):
        logging.info(msg)
        if Status.myth_job:
            Status.myth_job.setComment(msg)
    
    def set_progress(self, progress, eta):
        if Status.myth_job:
            Status.myth_job.setComment('Progress: {} %\nRemaining time: {}'.format(progress, eta))

    def set_status(self, new_status):
        logging.debug('Setting job status to {}'.format(new_status))
        if Status.myth_job:
            Status.myth_job.setStatus(new_status)

    def get_cmd(self):
        if Status.myth_job_id == 0:
            return Job.UNKNOWN
        # create new job object to pull current state from database
        return Job(Status.myth_job_id).cmds

    def get_chan_id(self):
        if Status.myth_job:
            return Status.myth_job.chanid
        return None

    def get_start_time(self):
        if Status.myth_job:
            return Status.myth_job.starttime
        return None

    def show_notification(self, msg, type):
        args = []
        args.append('mythutil')
        args.append('--notification')
        args.append('--origin')
        args.append('\"' + __file__ + '\"')
        args.append('--timeout')
        args.append('60')
        args.append('--message_text')
        args.append(msg)
        args.append('--type')
        args.append(type)
        cp = subprocess.run(args, capture_output=True, text=True)
        if cp.returncode != 0:
            logging.error(cp.stderr)
        if type == 'error':
            logging.error(msg)
        elif type == "warning":
            logging.warning(msg)
        elif type == "normal":
            logging.info(msg)

class VideoFilePath:
    def __init__(self):
        self.title = None
        self.subtitle = None
        self.season = 0
        self.episode = 0

    def build(self):
        dir_name = self._build_dir()
        if not dir_name:
            return None
        file_name = self._build_name()
        return os.path.join(dir_name, file_name)

    # Uses the following criteria by ascending priority
    # 1. Storage dir with maximum free space
    # 2. Directory matching recording title (useful for series)
    # 3. Directory containing files matching the title
    def _build_dir(self):
        db = MythDB()
        matched_dir_name = None
        title = "_".join(self.title.split())
        max_free_space = 0
        max_space_dir_name = None
        for sg in db.getStorageGroup(groupname='Videos'):
            # search given group
            if sg.local and os.path.isdir(sg.dirname):
                # get avaliable space of storage group partition
                # and use storage group with max. available space
                free_space = self._get_free_space(sg.dirname)
                logging.debug('Storage group {} -> space {}'.format(sg.dirname, free_space))
                if free_space > max_free_space:
                    max_space_dir_name = sg.dirname
                    max_free_space = free_space
                for root, dirs, files in os.walk(sg.dirname, followlinks=True):
                    # first check subdir for match
                    for d in dirs:
                        if self._match_title(title, d):
                            matched_dir_name = os.path.join(root, d)
                    # check file names for match
                    for f in files:
                        if self._match_title(title, f):
                            logging.debug('Using storage dir with files matching title')
                            return root
        # return directory matching title if found
        if matched_dir_name:
            logging.debug('Using storage dir matching title')
            return matched_dir_name
        # return storage directory with max free space
        logging.debug('Using storage dir with max. space')
        return max_space_dir_name

    def _build_name(self):
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

    def _get_free_space(self, file_name):
        stats = os.statvfs(file_name)
        return stats.f_bfree * stats.f_frsize

    # find storage directory by recording title
    def _match_title(self, title, name):
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

    def _abort(self, process):
        self.status.set_error('Aborting transcode due to timeout')
        process.kill()

    # start timer to abort transcode process if it hangs
    def _start_timer(self, timeout, cp):
        self._stop_timer()
        self.timer = Timer(timeout, self._abort, [cp])
        self.timer.start()

    def _stop_timer(self):
        if self.timer is not None:
            self.timer.cancel()
        self.timer = None

    def transcode(self, src_file, dst_file, preset, timeout):
        # get channel id and start time to identify recording
        chan_id = self.status.get_chan_id()
        start_time = self.status.get_start_time()
        if not start_time or not chan_id:
            logging.debug('Determine chanid and starttime from filename')
            # extract chanid and starttime from recording file name
            src_file_base_name,src_file_ext = os.path.splitext(os.path.basename(src_file))
            (chan_id, start_time) = src_file_base_name.split('_', 2)
            start_time = datetime.duck(start_time)

        # convert starttime from UTC
        start_time = datetime.fromnaiveutc(start_time)
        logging.debug('Using chanid={} and startime={}'.format(chan_id, start_time))

        # obtain cutlist
        try:
            db = MythDB()
            rec = Recorded((chan_id, start_time), db)
            cuts = rec.markup.getuncutlist()
        except MythError as err:
            logging.error('Could not read cutlist ({})'.format(err.message))
            return 1

        if len(cuts):
            logging.debug('Found {} cuts: {}'.format(len(cuts), cuts))

        if len(cuts) == 0:
            # transcode whole file directly
            res = self._transcode_part(src_file, dst_file, preset, timeout)
        if len(cuts) == 1:
            # transcode single part directly
            res = self._transcode_part(src_file, dst_file, preset, timeout, cuts[0])
        else:
            # transcode each part on its own
            cut_number = 1
            tmp_files = []
            dst_file_base_name,dst_file_ext = os.path.splitext(dst_file)
            for cut in cuts:
                dst_file_part = '{}_part_{}{}'.format(dst_file_base_name, cut_number, dst_file_ext)
                logging.info('Transcoding part {}/{} to {}'.format(cut_number, len(cuts), dst_file_part))
                res = self._transcode_part(src_file, dst_file_part, preset, timeout, cut)
                if res != 0:
                    break
                cut_number += 1
                tmp_files.append(dst_file_part)

            # merge transcoded parts
            if len(cuts) == len(tmp_files):
                logging.debug('Merging transcoded parts {}'.format(tmp_files))
                list_file = '{}_partlist.txt'.format(dst_file_base_name)
                with open(list_file, "w") as text_file:
                    for tmp_file in tmp_files:
                        text_file.write('file {}\n'.format(tmp_file))

                tmp_files.append(list_file)
                self.status.set_comment('Merging transcoded parts')

                args = []
                args.append('ffmpeg')
                args.append('-f')
                args.append('concat')
                args.append('-safe')
                args.append('0')
                args.append('-i')
                args.append(list_file)
                args.append('-c')
                args.append('copy')
                args.append(dst_file)
                logging.debug('Executing {}'.format(args))
                cp = subprocess.run(args, capture_output=True, text=True)
                res = cp.returncode
                if res != 0:
                    logging.error(cp.stderr)
                    tmp_files.append(dst_file)

            # delete transcoded parts
            for tmp_file in tmp_files:
                if os.path.isfile(tmp_file):
                    os.remove(tmp_file)

        if res == 0:
            # rescan videos
            self._add_video(src_file, dst_file)
            self._scan_videos()

        return res

    def _transcode_part(self, src_file, dst_file, preset, timeout, frames=None):
        # start the transcoding process
        args = []
        args.append('HandBrakeCLI')
        args.append('--preset')
        args.append(preset)
        args.append('-i')
        args.append(src_file)
        args.append('-o')
        args.append(dst_file)
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
        self._start_timer(timeout, cp)

        line = ''
        last_progress = 0
        while True:
            nl = cp.stdout.read(1)
            if nl == '' and cp.poll() is not None:
                break  # Aborted, no characters available, process died.
            if nl == '\n':
                last_token = ''
                progress = '0'
                eta = None
                # new line, restart abort timer
                self._start_timer(timeout, cp)
                for token in line.split():
                    if token == '%':
                        progress = last_token
                    if last_token == 'ETA':
                        eta = token.replace(')', '')
                    if eta and progress:
                        break
                    last_token = token
                if eta and int(float(progress)) > last_progress:
                    self.status.set_progress(progress, eta)
                    last_progress = int(float(progress))
                    # check if job was stopped externally
                    if self.status.get_cmd() == Job.STOP:
                        cp.kill()
                        break
                line = ''
            else:
                line += nl
        res = cp.wait()
        self._stop_timer()
        # remove video file on failure
        if res != 0:
            # print transcoding error output
            logging.error(cp.stderr.read())
            if os.path.isfile(dst_file):
                os.remove(dst_file)

        return res
        
    def _scan_videos(self):
        self.status.set_comment('Triggering video rescan')

        # scan videos
        args = []
        args.append('mythutil')
        args.append('--scanvideos')
        cp = subprocess.run(args, capture_output=True, text=True)
        if cp.returncode != 0:
            logging.error(cp.stderr)

    def _add_video(self, rec_path, vid_path):
        self.status.set_comment("Adding video and metadata to database")
        try:
            mbe = api.Send(host='localhost')

            rd = mbe.send(endpoint='Myth/GetHostName')
            host_name = rd['String']

            # find storage group from video path
            rd = mbe.send(endpoint='Myth/GetStorageGroupDirs', rest=f'HostName={host_name}&GroupName=Videos')
            storage_groups = rd['StorageGroupDirList']['StorageGroupDirs']
            vid_file = None
            for sg in storage_groups:
                sg_path = sg['DirName']
                if vid_path.startswith(sg_path):
                    vid_file = vid_path[len(sg_path):]
                    logging.debug(f'Found video in storage group {sg_path} -> {vid_file}')
                    break

            if not vid_file:
                return

            # add video
            data = {'HostName': host_name, 'FileName': vid_file}
            rd = mbe.send(endpoint='Video/AddVideo', postdata=data, opts={'debug': True, 'wrmi': True})
            if rd['bool'] == 'true':
                logging.info('Successfully added video')

            # get video id
            rd = mbe.send(endpoint='Video/GetVideoByFileName', rest=f'FileName={urllib.parse.quote(vid_file)}')
            vid_id = rd['VideoMetadataInfo']['Id']
            logging.debug(f'Got video id {vid_id}')

            # get recording id)
            rd = mbe.send(endpoint='Dvr/RecordedIdForPathname', rest=f'Pathname={urllib.parse.quote(rec_path)}')
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
            vid_length = self._get_video_length(vid_path)

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

    def _get_video_length(self, filename):
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

def format_file_size(num):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(num) < 1000.0:
            return "%3.1f %s" % (num, unit)
        num /= 1000.0
    return "%.1f %s" % (num, 'PB')


def main():
    parser = argparse.ArgumentParser(description='Transcoding recording and move to videos')
    parser.add_argument('-f', '--file', dest='rec_file', help='recording file name')
    parser.add_argument('-d', '--dir', dest='rec_dir', help='recording directory name')
    parser.add_argument('-p', '--path', dest='rec_path', help='recording path name')
    parser.add_argument('-t', '--title', dest='rec_title', help='recording title')
    parser.add_argument('-s', '--subtitle', dest='rec_subtitle', help='recording subtitle')
    parser.add_argument('-sn', '--season', dest='rec_season', default=0, type=int, help='recording season number')
    parser.add_argument('-en', '--episode', dest='rec_episode', default=0, type=int, help='recording episode number')
    parser.add_argument('-j', '--jobid', dest='job_id', help='mythtv job id')
    parser.add_argument('--preset', dest='preset', default='General/HQ 1080p30 Surround', help='Handbrake transcoding preset')
    parser.add_argument('--timeout', dest='timeout', default=300, type=int, help='timeout in seconds to abort transcoding process')
    parser.add_argument('-l', '--logfile', dest='log_file', default='', help='optional log file location')
    opts = parser.parse_args()

    if opts.log_file:
        logging.basicConfig(filename=opts.log_file, level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

    logging.debug('Command line: {}'.format(opts))

    status = Status(opts.job_id)

    rec_path = None
    if opts.rec_path:
        rec_path = opts.rec_path
    elif opts.rec_dir and opts.rec_file:
        rec_path = os.path.join(opts.rec_dir, opts.rec_file)
    if not rec_path:
        status.set_error('Recording path or recording directoy + recording file not specified')
        sys.exit(1)
    if not os.path.isfile(rec_path):
        status.set_error('Input recording file does not exist')
        sys.exit(1)

    if opts.rec_title is None and opts.rec_subtitle is None:
        status.set_error('Title and/or subtitle not specified')
        sys.exit(1)

    # build output file path
    path_builder = VideoFilePath()
    path_builder.title = opts.rec_title
    path_builder.subtitle = opts.rec_subtitle
    path_builder.season = opts.rec_season
    path_builder.episode = opts.rec_episode
    vid_path = path_builder.build()
    if not vid_path:
        status.set_error('Could not find video storage directory')
        sys.exit(2)
    if os.path.isfile(vid_path):
        status.set_error('Output video file already exists: \"{}\"'.format(vid_path))
        sys.exit(3)

    status.set_status(Job.RUNNING)

    # start transcoding
    logging.info('Started transcoding \"{}\"'.format(opts.rec_title))
    logging.info('Source recording file : {}'.format(rec_path))
    logging.info('Destination video file: {}'.format(vid_path))
    res = Transcoder().transcode(rec_path, vid_path, opts.preset, opts.timeout)
    if status.get_cmd() == Job.STOP:
        status.set_status(Job.CANCELLED)
        status.set_comment('Stopped transcoding')
        status.show_notification('Stopped transcoding \"{}\"'.format(opts.rec_title), 'warning')
        sys.exit(4)
    elif res != 0:
        status.set_error('Failed transcoding (error {})'.format(res))
        status.show_notification('Failed transcoding \"{}\" (error {})'.format(opts.rec_title, res), 'error')
        sys.exit(res)

    rec_size = os.stat(rec_path).st_size
    vid_size = os.stat(vid_path).st_size
    size_status = format_file_size(rec_size) + ' => ' + format_file_size(vid_size)
    status.show_notification('Finished transcoding \"{}\"'.format(opts.rec_title) + '\n' + size_status, 'normal')
    status.set_comment('Finished transcoding\n' + size_status)
    status.set_status(Job.FINISHED)

    # .. the end
    sys.exit(0)


if __name__ == "__main__":
    main()
