#!/usr/bin/env python3

""" Transcodes a MythTV recording and puts it into the video storage """

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
    """ Manages status reporting """
    myth_job = None
    myth_job_id = 0

    def __init__(self, job_id=0):
        if job_id and not Status.myth_job:
            Status.myth_job_id = job_id
            Status.myth_job = Job(job_id)
            Status.myth_job.update(status=Job.STARTING)
            self.set_comment('Starting job...')

    def set_error(self, msg):
        """ Set an error state to the myth job object """
        logging.error(msg)
        self.set_comment(msg)
        self.set_status(Job.ERRORED)

    def set_comment(self, msg):
        """ Sets a comment text to the myth job object """
        logging.info(msg)
        if Status.myth_job:
            Status.myth_job.setComment(msg)

    def set_progress(self, progress, eta):
        """ Sets progress as a comment to the myth job object """
        if Status.myth_job:
            Status.myth_job.setComment(f'Progress: {progress} %\nRemaining time: {eta}')

    def set_status(self, new_status):
        """ Sets a state to the myth job object """
        logging.debug('Setting job status to %s', new_status)
        if Status.myth_job:
            Status.myth_job.setStatus(new_status)

    def get_cmd(self):
        """ Reads the current myth job state from the database """
        if Status.myth_job_id == 0:
            return Job.UNKNOWN
        # create new job object to pull current state from database
        return Job(Status.myth_job_id).cmds

    def get_chan_id(self):
        """ Reads the chanid from the myth job object """
        if Status.myth_job:
            return Status.myth_job.chanid
        return None

    def get_start_time(self):
        """ Reads the starttime from the myth job object """
        if Status.myth_job:
            return Status.myth_job.starttime
        return None

    def show_notification(self, msg, msg_type):
        """ Displays a visual notification on active frontends """
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
        args.append(msg_type)
        try:
            subprocess.run(args, capture_output=True, text=True, check=True)
        except subprocess.CalledProcessError as error:
            logging.error(error.stderr)
        if msg_type == 'error':
            logging.error(msg)
        elif msg_type == "warning":
            logging.warning(msg)
        elif msg_type == "normal":
            logging.info(msg)

class VideoFilePath:
    """ Build video file name from title, subtitle and season metadata
        Also finds best matching storage group from different criteria.
    """
    def __init__(self, title, subtitle = None, season = 0, episode = 0):
        self.title = title
        self.subtitle = subtitle
        self.season = season
        self.episode = episode

    def build(self):
        """ Builds the video file path """
        dir_name = self._build_dir()
        if not dir_name:
            return None
        file_name = self._build_name()
        return os.path.join(dir_name, file_name)

    def _build_dir(self):
        """ Builds the video file directory.
            It scans all video storage dirs to find the best
            one using the following criteria by ascending priority:
            1. Storage dir with maximum free space
            2. Directory matching recording title (useful for series)
            3. Directory containing files matching the title
        """
        myth_db = MythDB()
        matched_dir_name = None
        title = "_".join(self.title.split())
        max_free_space = 0
        max_space_dir_name = None
        for storage_group in myth_db.getStorageGroup(groupname='Videos'):
            # search given group
            if storage_group.local and os.path.isdir(storage_group.dirname):
                # get avaliable space of storage group partition
                # and use storage group with max. available space
                free_space = self._get_free_space(storage_group.dirname)
                logging.debug('Storage group %s -> space %s', storage_group.dirname, free_space)
                if free_space > max_free_space:
                    max_space_dir_name = storage_group.dirname
                    max_free_space = free_space
                for sg_root, sg_dirs, sg_files in os.walk(storage_group.dirname, followlinks=True):
                    # first check subdir for match
                    for sg_dir in sg_dirs:
                        if self._match_title(title, sg_dir):
                            matched_dir_name = os.path.join(sg_root, sg_dir)
                    # check file names for match
                    for sg_file in sg_files:
                        if self._match_title(title, sg_file):
                            logging.debug('Using storage dir with files matching title')
                            return sg_root
        # return directory matching title if found
        if matched_dir_name:
            logging.debug('Using storage dir matching title')
            return matched_dir_name
        # return storage directory with max free space
        logging.debug('Using storage dir with max. space')
        return max_space_dir_name

    def _build_name(self):
        """ Builds video file name: "The_title(_-_|_SxxEyy_][The_Subtitle].m4v" """
        parts = []
        if self.title and self.title != "":
            parts.append(self.title)
        if self.season > 0 and self.episode > 0:
            parts.append(f'S{self.season:02}E{self.episode:02}')
        elif self.subtitle and self.subtitle != "":
            parts.append('-')
        if self.subtitle and self.subtitle != "":
            parts.append(self.subtitle)
        return "_".join(' '.join(parts).split()) + ".m4v"

    def _get_free_space(self, file_name):
        """ Returns the free space of the partition of the specified file/directory """
        stats = os.statvfs(file_name)
        return stats.f_bfree * stats.f_frsize

    def _match_title(self, title, name):
        """ Checks if file or directory name starts with specified title """
        simplified_title = title.lower()
        simplified_name = name.lower()
        for char in (' ', '_', '-'):
            simplified_name = simplified_name.replace(char, '')
            simplified_title = simplified_title.replace(char, '')
        return simplified_name.startswith(simplified_title)


class Transcoder:
    """ Handles transcoding a recording to a video file """
    def __init__(self, src_file, dst_file, preset, timeout):
        self.status = Status()
        self.timer = None
        self.src_file = src_file
        self.dst_file = dst_file
        self.preset = preset
        self.timeout = timeout

    def _abort(self, process):
        """ Abort transcoding after timeout """
        self.status.set_error('Aborting transcode due to timeout')
        process.kill()

    def _start_timer(self, process):
        """ Start timer to abort transcode process if it hangs """
        self._stop_timer()
        self.timer = Timer(self.timeout, self._abort, [process])
        self.timer.start()

    def _stop_timer(self):
        """ Stop the abort transcoding timer """
        if self.timer is not None:
            self.timer.cancel()
        self.timer = None

    def transcode(self):
        """ Transcode the source file to the destination file using the specified preset
            The cutlist of the recording (source file) is used to transcode
            multiple parts of the recording if neccessary and then merged into the final
            destination file.
            At the end the video is added to the database and metadata of the recording
            is copied to the video metadata.
        """
        # get channel id and start time to identify recording
        chan_id = self.status.get_chan_id()
        start_time = self.status.get_start_time()
        if not start_time or not chan_id:
            logging.debug('Determine chanid and starttime from filename')
            # extract chanid and starttime from recording file name
            src_file_base_name = os.path.splitext(os.path.basename(self.src_file))[0]
            (chan_id, start_time) = src_file_base_name.split('_', 2)
            start_time = datetime.duck(start_time)

        # convert starttime from UTC
        start_time = datetime.fromnaiveutc(start_time)
        logging.debug('Using chanid=%s and starttime=%s', chan_id, start_time)

        # obtain cutlist
        try:
            myth_rec = Recorded((chan_id, start_time), MythDB())
            cuts = myth_rec.markup.getuncutlist()
        except MythError as err:
            logging.error('Could not read cutlist (%s)', err.message)
            return 1

        if cuts:
            logging.debug('Found %s cuts: %s', len(cuts), cuts)

        if not cuts:
            # transcode whole file directly
            res = self._transcode_single()
        elif len(cuts) == 1:
            # transcode single part directly
            res = self._transcode_single(cuts[0])
        else:
            # transcode each part on its own
            res = self._transcode_multiple(cuts)

        if res == 0:
            # rescan videos
            self._add_video(self.src_file, self.dst_file)
            self._scan_videos()

        return res

    def _transcode_multiple(self, cuts):
        # transcode each part on its own
        cut_number = 1
        tmp_files = []
        dst_file_base_name,dst_file_ext = os.path.splitext(self.dst_file)
        for cut in cuts:
            dst_file_part = f'{dst_file_base_name}_part_{cut_number}{dst_file_ext}'
            logging.info('Transcoding part %s/%s to %s', cut_number, len(cuts), dst_file_part)
            res = self._transcode_single(cut)
            if res != 0:
                break
            cut_number += 1
            tmp_files.append(dst_file_part)

        # merge transcoded parts
        if len(cuts) == len(tmp_files):
            res = self._merge_parts(tmp_files, self.dst_file)

        # delete transcoded parts
        for tmp_file in tmp_files:
            if os.path.isfile(tmp_file):
                os.remove(tmp_file)
        return res

    def _transcode_single(self, frames=None):
        """ Start HandBrake to transcodes all or a single part (identified by
            start and end frame) of the source file
            A timer is used to abort the transcoding if there was no progress
            detected within a specfied timeout period.
        """
        # start the transcoding process
        args = []
        args.append('HandBrakeCLI')
        args.append('--preset')
        args.append(self.preset)
        args.append('-i')
        args.append(self.src_file)
        args.append('-o')
        args.append(self.dst_file)
        if frames:
            logging.debug('Transcoding from frame %s to %s', frames[0], frames[1])
            # pass start and end position of remaining part to handbrake
            args.append('--start-at')
            args.append(f'frame:{frames[0]}')
            # stop it relative to start position
            args.append('--stop-at')
            args.append(f'frame:{frames[1]-frames[0]}')

        logging.debug('Executing %s', args)
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # start timer to abort transcode process if it hangs
        self._start_timer(proc)

        line = ''
        last_progress = 0
        while True:
            char = proc.stdout.read(1)
            if char == '' and proc.poll():
                break  # Aborted, no characters available, process died.
            if char == '\n':
                last_token = ''
                progress = None
                eta = None
                # new line, restart abort timer
                self._start_timer(proc)
                for token in line.split():
                    if token == '%':
                        progress = int(float(last_token))
                    if last_token == 'ETA':
                        eta = token.replace(')', '')
                    if eta and progress and progress > last_progress:
                        self.status.set_progress(progress, eta)
                        last_progress = progress
                        break
                    last_token = token
                # check if job was stopped externally
                if self.status.get_cmd() == Job.STOP:
                    proc.kill()
                    break
                line = ''
            else:
                line += char
        res = proc.wait()
        self._stop_timer()
        # remove video file on failure
        if res != 0:
            # print transcoding error output
            logging.error(proc.stderr.read())
            if os.path.isfile(self.dst_file):
                os.remove(self.dst_file)

        return res

    def _merge_parts(self, parts, dst_file):
        logging.debug('Merging transcoded parts %s', parts)
        list_file = f'{os.path.splitext(dst_file)[0]}_partlist.txt'
        with open(list_file, "w") as text_file:
            for part in parts:
                text_file.write(f'file {part}\n')

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
        logging.debug('Executing %s', args)
        try:
            proc = subprocess.run(args, capture_output=True, text=True, check=True)
        except subprocess.CalledProcessError as error:
            logging.error(error.stderr)
            os.remove(dst_file)
        finally:
            os.remove(list_file)

        return proc.returncode

    def _scan_videos(self):
        """ Triggers a video scan using mythutil """
        self.status.set_comment('Triggering video rescan')

        # scan videos
        args = []
        args.append('mythutil')
        args.append('--scanvideos')
        try:
            subprocess.run(args, capture_output=True, text=True, check=True)
        except subprocess.CalledProcessError as error:
            logging.error(error.stderr)

    def _add_video(self, rec_path, vid_path):
        """ Adds the video to the database and copies recording metadata"""
        self.status.set_comment("Adding video and metadata to database")
        try:
            mbe = api.Send(host='localhost')

            result = mbe.send(endpoint='Myth/GetHostName')
            host_name = result['String']

            # find storage group from video path
            result = mbe.send(endpoint='Myth/GetStorageGroupDirs',
                          rest=f'HostName={host_name}&GroupName=Videos')
            storage_groups = result['StorageGroupDirList']['StorageGroupDirs']
            vid_file = None
            for sg_data in storage_groups:
                sg_path = sg_data['DirName']
                if vid_path.startswith(sg_path):
                    vid_file = vid_path[len(sg_path):]
                    logging.debug('Found video in storage group %s -> %s', sg_path, vid_file)
                    break

            if not vid_file:
                return

            # add video
            data = {'HostName': host_name, 'FileName': vid_file}
            result = mbe.send(endpoint='Video/AddVideo', postdata=data,
                          opts={'debug': True, 'wrmi': True})
            if result['bool'] == 'true':
                logging.info('Successfully added video')

            # get video id
            result = mbe.send(endpoint='Video/GetVideoByFileName',
                          rest=f'FileName={urllib.parse.quote(vid_file)}')
            vid_id = result['VideoMetadataInfo']['Id']
            logging.debug('Got video id %s', vid_id)

            # get recording id)
            result = mbe.send(endpoint='Dvr/RecordedIdForPathname',
                          rest=f'Pathname={urllib.parse.quote(rec_path)}')
            rec_id = result['int']
            logging.debug('Got recording id %s', rec_id)

            # get recording metadata
            result = mbe.send(endpoint='Dvr/GetRecorded', rest=f'RecordedId={rec_id}')

            # collect metadata
            description = result['Program']['Description']
            director = []
            actors = []
            for member in result['Program']['Cast']['CastMembers']:
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
            if director:
                data['Director'] = ', '.join(director)
            if actors:
                data['Cast'] = ', '.join(actors)
            if len(data) > 1:
                result = mbe.send(endpoint='Video/UpdateVideoMetadata', postdata=data,
                              opts={'debug': True, 'wrmi': True})
                if result['bool'] == 'true':
                    logging.info('Successfully updated video metadata')
        except RuntimeError as error:
            logging.error('\nFatal error: "%s"', error)

    def _get_video_length(self, filename):
        """ Determines the video length using ffprobe """
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
        logging.debug('Executing %s', args)
        try:
            proc = subprocess.run(args, capture_output=True, text=True, check=True)
            return int(math.ceil(float(proc.stdout) / 60.0))
        except subprocess.CalledProcessError as error:
            logging.error(error.stderr)
            return 0
        except ValueError:
            return 0

def format_file_size(num):
    """ Formats the given number as a file size """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(num) < 1000.0:
            return "%3.1f %s" % (num, unit)
        num /= 1000.0
    return "%.1f %s" % (num, 'PB')

def parse_arguments():
    """ Parses command line arguments """
    parser = argparse.ArgumentParser(description='Transcode recording and move it to video storage')
    parser.add_argument('-f', '--file', dest='rec_file', help='recording file name')
    parser.add_argument('-d', '--dir', dest='rec_dir', help='recording directory name')
    parser.add_argument('-p', '--path', dest='rec_path', help='recording path name')
    parser.add_argument('-t', '--title', dest='rec_title', help='recording title')
    parser.add_argument('-s', '--subtitle', dest='rec_subtitle', help='recording subtitle')
    parser.add_argument('-sn', '--season', dest='rec_season', default=0, type=int,
                        help='recording season number')
    parser.add_argument('-en', '--episode', dest='rec_episode', default=0, type=int,
                        help='recording episode number')
    parser.add_argument('-j', '--jobid', dest='job_id', help='mythtv job id')
    parser.add_argument('--preset', dest='preset', default='General/HQ 1080p30 Surround',
                        help='Handbrake transcoding preset')
    parser.add_argument('--timeout', dest='timeout', default=300, type=int,
                        help='timeout in seconds to abort transcoding process')
    parser.add_argument('-l', '--logfile', dest='log_file', default='',
                        help='optional log file location')

    return parser.parse_args()

def main():
    """ Main entry function """
    opts = parse_arguments()

    if opts.log_file:
        logging.basicConfig(filename=opts.log_file, level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s: %(message)s')

    logging.debug('Command line: %s', opts)

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
    path_builder = VideoFilePath(opts.rec_title, opts.rec_subtitle,
                                 opts.rec_season, opts.rec_episode)
    vid_path = path_builder.build()
    if not vid_path:
        status.set_error('Could not find video storage directory')
        sys.exit(2)
    if os.path.isfile(vid_path):
        status.set_error(f'Output video file already exists: \"{vid_path}\"')
        sys.exit(3)

    status.set_status(Job.RUNNING)

    # start transcoding
    logging.info('Started transcoding \"%s\"', opts.rec_title)
    logging.info('Source recording file : %s', rec_path)
    logging.info('Destination video file: %s', vid_path)
    res = Transcoder(rec_path, vid_path, opts.preset, opts.timeout).transcode()
    if status.get_cmd() == Job.STOP:
        status.set_status(Job.CANCELLED)
        status.set_comment('Stopped transcoding')
        status.show_notification(f'Stopped transcoding \"{opts.rec_title}\"', 'warning')
        sys.exit(4)
    elif res != 0:
        status.set_error(f'Failed transcoding (error {res})')
        status.show_notification(f'Failed transcoding \"{opts.rec_title}\" (error {res})', 'error')
        sys.exit(res)

    rec_size = format_file_size(os.stat(rec_path).st_size)
    vid_size = format_file_size(os.stat(vid_path).st_size)
    size_status = f'{rec_size} => {vid_size}'
    status.show_notification(f'Finished transcoding "{opts.rec_title}"\n{size_status}', 'normal')
    status.set_comment(f'Finished transcoding\n{size_status}')
    status.set_status(Job.FINISHED)

    # .. the end
    sys.exit(0)


if __name__ == "__main__":
    main()
