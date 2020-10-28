#!/usr/bin/env python3

""" Transcodes a MythTV recording and puts it into the video storage """

import argparse
import sys
import os
import subprocess
import logging
import math
import urllib.parse
import re
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

    @staticmethod
    def set_error(msg):
        """ Set an error state to the myth job object """
        logging.error(msg)
        Status.set_comment(msg)
        Status.set_status(Job.ERRORED)

    @staticmethod
    def set_comment(msg):
        """ Sets a comment text to the myth job object """
        logging.info(msg)
        if Status.myth_job:
            Status.myth_job.setComment(msg)

    @staticmethod
    def set_progress(progress, eta):
        """ Sets progress as a comment to the myth job object """
        if Status.myth_job:
            Status.myth_job.setComment(f'Progress: {progress} %\nRemaining time: {eta}')

    @staticmethod
    def set_status(new_status):
        """ Sets a state to the myth job object """
        logging.debug('Setting job status to %s', new_status)
        if Status.myth_job:
            Status.myth_job.setStatus(new_status)

    @staticmethod
    def get_cmd():
        """ Reads the current myth job state from the database """
        if Status.myth_job_id == 0:
            return Job.UNKNOWN
        # create new job object to pull current state from database
        return Job(Status.myth_job_id).cmds

    @staticmethod
    def get_chan_id():
        """ Reads the chanid from the myth job object """
        if Status.myth_job:
            return Status.myth_job.chanid
        return None

    @staticmethod
    def get_start_time():
        """ Reads the starttime from the myth job object """
        if Status.myth_job:
            return Status.myth_job.starttime
        return None


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
                free_space = Util.get_free_space(storage_group.dirname)
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

    @staticmethod
    def _match_title(title, name):
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
        self.timer = None
        self.src_file = src_file
        self.dst_file = dst_file
        self.preset = preset
        self.timeout = timeout

    @staticmethod
    def _abort(process):
        """ Abort transcoding after timeout """
        Status.set_error('Aborting transcode due to timeout')
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
        chan_id = Status.get_chan_id()
        start_time = Status.get_start_time()
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
            res = self._transcode_single(self.dst_file)
        elif len(cuts) == 1:
            # transcode single part directly
            res = self._transcode_single(self.dst_file, cuts[0])
        else:
            # transcode each part on its own
            res = self._transcode_multiple(cuts)

        return res

    def _transcode_multiple(self, cuts):
        # transcode each part on its own
        cut_number = 1
        tmp_files = []
        dst_file_base_name,dst_file_ext = os.path.splitext(self.dst_file)
        for cut in cuts:
            dst_file_part = f'{dst_file_base_name}_part_{cut_number}{dst_file_ext}'
            logging.info('Transcoding part %s/%s to %s', cut_number, len(cuts), dst_file_part)
            res = self._transcode_single(dst_file_part, cut)
            if res != 0:
                break
            cut_number += 1
            tmp_files.append(dst_file_part)

        # merge transcoded parts
        if len(cuts) == len(tmp_files):
            res = self._merge_parts(tmp_files, self.dst_file)

        # delete transcoded parts
        for tmp_file in tmp_files:
            Util.remove_file(tmp_file)

        return res

    def _transcode_single(self, dst_file, frames=None):
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
        args.append(dst_file)
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

        # regex pattern to find prograss and ETA in output line
        pattern = re.compile(r"([\d]*\.[\d]*)(?=\s\%)(.*fps.*)(?<=[ETA]\s)([\d]*h[\d]*m[\d]*s)")

        line = ''
        last_progress = 0
        while True:
            char = proc.stdout.read(1)
            if char == '' and proc.poll() is not None:
                logging.debug("Process has died")
                break  # Aborted, no characters available, process died.
            if char == '\n':
                # new line, restart abort timer
                self._start_timer(proc)

                progress = None
                eta = None
                try:
                    if matched := re.search(pattern, line):
                        progress = int(float(matched.group(1)))
                        eta = matched.group(3)
                except IndexError:
                    pass
                else:
                    if progress and eta and progress > last_progress:
                        Status.set_progress(progress, eta)
                        last_progress = progress
                line = ''
                # check if job was stopped externally
                if Status.get_cmd() == Job.STOP:
                    proc.kill()
                    break
            else:
                line += char
        proc.wait()
        self._stop_timer()
        # remove video file on failure
        if proc.returncode != 0:
            # print transcoding error output
            logging.error(proc.stderr.read())
            Util.remove_file(dst_file)

        return proc.returncode

    @staticmethod
    def _merge_parts(parts, dst_file):
        logging.debug('Merging transcoded parts %s', parts)
        list_file = f'{os.path.splitext(dst_file)[0]}_partlist.txt'
        with open(list_file, "w") as text_file:
            for part in parts:
                text_file.write(f'file {part}\n')

        Status.set_comment('Merging transcoded parts')

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
            Util.remove_file(dst_file)
        finally:
            Util.remove_file(list_file)

        return proc.returncode


class Backend:
    """ Handles sending and receiving data to/from the Mythtv backend """
    def __init__(self):
        try:
            self.mbe = api.Send(host='localhost')
            result = self.mbe.send(
                endpoint='Myth/GetHostName'
            )
            self.host_name = result['String']
        except RuntimeError as error:
            logging.error('\nFatal error: "%s"', error)
        self.post_opts = {'wrmi': True}
        if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
            self.post_opts['debug'] = True

    def get_storage_group_data(self, group_name=None):
        """ Retrieve storage group data from backend """
        if group_name:
            data =f'HostName={self.host_name}&GroupName={group_name}'
        else:
            data =f'HostName={self.host_name}'
        try:
            result = self.mbe.send(
                endpoint='Myth/GetStorageGroupDirs', rest=data
            )
            return result
        except RuntimeError as error:
            logging.error('\nFatal error: "%s"', error)
            return None

    def get_storage_dirs(self, group_name=None):
        """ Returns list of storage group directories """
        data = self.get_storage_group_data(group_name)
        if not data:
            return []
        storage_groups = data['StorageGroupDirList']['StorageGroupDirs']
        dirs = []
        for sg_data in storage_groups:
            dirs.append(sg_data['DirName'])
        return dirs

    def add_video(self, vid_path):
        """ Adds specified video to the database
            The path must be an absolute path.
        """
        if not vid_path:
            return False
        try:
            data = {'HostName': self.host_name, 'FileName': vid_path}
            result = self.mbe.send(
                endpoint='Video/AddVideo', postdata=data, opts=self.post_opts
            )
            if result['bool'] == 'true':
                return True
        except RuntimeError as error:
            logging.error('\nFatal error: "%s"', error)
        return False

    def get_video_id(self, vid_file):
        """ Retrieves the video id of the specified video file
            The video file must be relative to one of the video
            storage dirs.
        """
        try:
            data = f'FileName={urllib.parse.quote(vid_file)}'
            result = self.mbe.send(
                endpoint='Video/GetVideoByFileName', rest=data
            )
            return result['VideoMetadataInfo']['Id']
        except RuntimeError as error:
            logging.error('\nFatal error: "%s"', error)
        return None

    def get_recording_id(self, rec_path):
        """ Retrieves recording id of specified recording file """
        try:
            data = f'Pathname={urllib.parse.quote(rec_path)}'
            result = self.mbe.send(
                endpoint='Dvr/RecordedIdForPathname', rest=data
            )
            return result['int']
        except RuntimeError as error:
            logging.error('\nFatal error: "%s"', error)
        return None

    def get_recording_metadata(self, rec_id):
        """ Retrieves metadata of the specified recording """
        try:
            data = f'RecordedId={rec_id}'
            result = self.mbe.send(
                endpoint='Dvr/GetRecorded', rest=data
            )
            return result
        except RuntimeError as error:
            logging.error('\nFatal error: "%s"', error)
        return None

    def update_video_metadata(self, vid_id, data):
        """ Updates metadata of the specified video """
        try:
            if not data:
                return False
            data['Id'] = vid_id
            result = self.mbe.send(
                endpoint='Video/UpdateVideoMetadata', postdata=data, opts=self.post_opts
            )
            if result['bool'] == 'true':
                return True
        except RuntimeError as error:
            logging.error('\nFatal error: "%s"', error)
        return False

class Util:
    """ Utility class """
    @staticmethod
    def format_file_size(num):
        """ Formats the given number as a file size """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if abs(num) < 1000.0:
                return "%3.1f %s" % (num, unit)
            num /= 1000.0
        return "%.1f %s" % (num, 'PB')

    @staticmethod
    def get_free_space(file_name):
        """ Returns the free space of the partition of the specified file/directory """
        stats = os.statvfs(file_name)
        return stats.f_bfree * stats.f_frsize

    @staticmethod
    def get_video_length(filename):
        """ Determines the video length using ffprobe
            Returns the video length in minutes.
        """
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

    @staticmethod
    def add_video(rec_path, vid_path):
        """ Adds the video to the database and copies recording metadata """
        Status().set_comment('Adding video to database')

        mbe = Backend()

        # find video path relative to storage dir
        vid_file = None
        for sg_path in mbe.get_storage_dirs('Videos'):
            if vid_path.startswith(sg_path):
                vid_file = vid_path[len(sg_path):]
                logging.debug('Found video in storage group %s -> %s', sg_path, vid_file)
                break

        # add video to database
        if mbe.add_video(vid_file):
            logging.info('Successfully added video')
        else:
            return

        vid_id = mbe.get_video_id(vid_file)
        logging.debug('Got video id %s', vid_id)
        rec_id = mbe.get_recording_id(rec_path)
        logging.debug('Got recording id %s', rec_id)

        rec_data = mbe.get_recording_metadata(rec_id)

        # collect metadata
        description = rec_data['Program']['Description']
        director = []
        actors = []
        for member in rec_data['Program']['Cast']['CastMembers']:
            if member['Role'] == 'director':
                director.append(member['Name'])
            if member['Role'] == 'actor':
                actors.append(member['Name'])
        vid_length = Util.get_video_length(vid_path)

        # update video metadata
        data = {}
        if description:
            data['Plot'] = description
        if vid_length >= 1:
            data['Length'] = vid_length
        if director:
            data['Director'] = ', '.join(director)
        if actors:
            data['Cast'] = ', '.join(actors)
        if mbe.update_video_metadata(vid_id, data):
            logging.info('Successfully updated video metadata')

    @staticmethod
    def scan_videos():
        """ Triggers a video scan using mythutil """
        Status().set_comment('Triggering video rescan')
        args = []
        args.append('mythutil')
        args.append('--scanvideos')
        try:
            subprocess.run(args, capture_output=True, text=True, check=True)
        except subprocess.CalledProcessError as error:
            logging.error(error.stderr)

    @staticmethod
    def show_notification(msg, msg_type):
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

    @staticmethod
    def remove_file(filename):
        """ Safely removes specified file """
        if os.path.isfile(filename):
            logging.debug('Removing file %s', filename)
            os.remove(filename)


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

    args = parser.parse_args()

    if args.log_file:
        logging.basicConfig(filename=args.log_file, level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s: %(message)s')

    logging.debug('Command line: %s', args)

    return args

def main():
    """ Main entry function """
    opts = parse_arguments()

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
        Util.show_notification(f'Stopped transcoding \"{opts.rec_title}\"', 'warning')
        sys.exit(4)
    elif res == 0:
        Util.add_video(rec_path, vid_path)
        Util.scan_videos()
    elif res != 0:
        status.set_error(f'Failed transcoding (error {res})')
        Util.show_notification(f'Failed transcoding \"{opts.rec_title}\" (error {res})', 'error')
        sys.exit(res)

    rec_size = Util.format_file_size(os.stat(rec_path).st_size)
    vid_size = Util.format_file_size(os.stat(vid_path).st_size)
    size_status = f'{rec_size} => {vid_size}'
    Util.show_notification(f'Finished transcoding "{opts.rec_title}"\n{size_status}', 'normal')
    status.set_comment(f'Finished transcoding\n{size_status}')
    status.set_status(Job.FINISHED)

    # .. the end
    sys.exit(0)


if __name__ == "__main__":
    main()
