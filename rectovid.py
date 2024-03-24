#!/usr/bin/env python3

""" Transcodes a MythTV recording and puts it into the video storage """

import argparse
import sys
import os
import subprocess
import logging
import logging.handlers
import math
import urllib.parse
import re
import json
import time
import configparser
import shlex
from threading import Timer
from MythTV import Job
from MythTV.services_api import send as api

sys.path.append("/usr/bin")


class Status:
    """ Manages status reporting """
    _myth_job = None
    _myth_job_id = 0
    _subprogresses = []
    _cur_subprogress = 0
    _progress_start = None
    _last_progress = None

    def __init__(self, job_id=0):
        if job_id and not Status._myth_job:
            Status._myth_job_id = job_id
            Status._myth_job = Job(job_id)
            Status._myth_job.update(status=Job.STARTING)
            self.set_comment('Starting job...')

    @staticmethod
    def set_error(msg):
        """ Set an error state to the myth job object """
        logging.error(msg)
        Status.set_comment(msg, log=False)
        Status.set_status(Job.ERRORED)

    @staticmethod
    def set_comment(msg, log=True):
        """ Sets a comment text to the myth job object """
        if log:
            logging.info(msg)
        if Status._myth_job:
            Status._myth_job.setComment(msg)

    @staticmethod
    def add_subprogress(duration):
        """ Adds a subprogress """
        Status._subprogresses.append({'Duration': duration, 'Start': 0, 'End': 0})
        total_duration = 0.0
        for sub in Status._subprogresses:
            total_duration += float(sub['Duration'])
        current_duration = 0.0
        for sub in Status._subprogresses:
            sub['Start'] = current_duration / total_duration
            current_duration += float(sub['Duration'])
            sub['End'] = current_duration / total_duration

    @staticmethod
    def next_subprogress():
        """ Switches to next subprogress """
        if Status._subprogresses:
            Status._cur_subprogress += 1

    @staticmethod
    def reset_progress():
        """ Clears subprogress list and resets start time """
        Status._subprogresses = []
        Status._cur_subprogress = 0
        Status._progress_start = None
        Status._last_progress = None

    @staticmethod
    def init_progress():
        """ Initializes progress start time """
        Status._progress_start = time.time()
        logging.debug(Status._subprogresses)

    @staticmethod
    def set_progress(progress):
        """ Sets progress as a comment to the myth job object """
        if not progress:
            return
        if Status._subprogresses and Status._cur_subprogress < len(Status._subprogresses):
            sub = Status._subprogresses[Status._cur_subprogress]
            progress = sub['Start']*100.0 + (sub['End'] - sub['Start']) * progress
        if Status._last_progress and Status._last_progress == int(progress):
            return

        Status._last_progress = int(progress)

        eta = None
        if Status._progress_start and progress > 0.0:
            time_spent = time.time() - Status._progress_start
            time_left = (100.0 - progress) * time_spent / progress
            eta = time.strftime('%H:%M:%S', time.gmtime(time_left))

        if Status._myth_job:
            if eta:
                Status._myth_job.setComment(f'Progress: {int(progress)} %\nRemaining time: {eta}')
            else:
                Status._myth_job.setComment(f'Progress: {int(progress)} %')

    @staticmethod
    def set_status(new_status):
        """ Sets a state to the myth job object """
        if Status._myth_job:
            logging.debug('Setting job status to %s', new_status)
            Status._myth_job.setStatus(new_status)

    @staticmethod
    def get_status():
        """ Gets state of the myth job object """
        if Status._myth_job:
            return Status._myth_job.status
        return Job.UNKNOWN

    @staticmethod
    def get_cmd():
        """ Reads the current myth job state from the database """
        if Status._myth_job_id == 0:
            return Job.UNKNOWN
        # create new job object to pull current state from database
        return Job(Status._myth_job_id).cmds

    @staticmethod
    def canceled():
        """ Checks if myth job object has been stopped/canceled """
        return Status.get_cmd() == Job.STOP or Status.get_status() == Job.CANCELLED

    @staticmethod
    def failed():
        """ Checks if myth job object has error state """
        return Status.get_status() == Job.ERRORED


class VideoFilePath:
    """ Build video file name from title, subtitle and season metadata
        Also finds best matching storage group from different criteria.
    """
    def __init__(self, recording):
        self.recording = recording
        self.storage_dir, self.video_dir = self._find_dir()
        self.path = None
        if self.storage_dir:
            self.video_file = self._build_name()
            self.path = os.path.join(os.path.join(self.storage_dir, self.video_dir), self.video_file)

    def __str__(self):
        return self.path if self.path else ''

    def _find_dir(self):
        """ Builds the video file directory.
            It scans all video storage dirs to find the best
            one using the following criteria by ascending priority:
            1. Storage dir with maximum free space
            2. Directory matching recording title (useful for series)
            3. Directory containing files matching the title
        """
        mbe = Backend()
        matched_dir_name = None
        matched_storage_dir = None
        max_free_space = 0
        max_space_storage_dir = None
        rec_size = int(Util.get_file_size(self.recording.path) / 1000.0)
        logging.debug("Recording %s -> size %s KiB", self.recording.path, rec_size)
        for storage_group in mbe.get_storage_group_data(group_name='Videos'):
            if storage_group['HostName'] != mbe.host_name:
                continue
            if storage_group['DirWrite'] != True:
                continue
            storage_dir = storage_group['DirName']
            # search given group
            if not os.path.isdir(storage_dir):
                continue
            # get avaliable space of storage group partition
            # and use storage group with max. available space
            free_space = int(storage_group['KiBFree'])
            logging.debug('Storage group %s -> space %s KiB', storage_dir, free_space)
            # check if recording fits into storage group
            if rec_size > free_space:
                logging.warning('Recording size exceeds free space on storage group, skipping')
                continue
            if free_space > max_free_space:
                max_space_storage_dir = storage_dir
                max_free_space = free_space
            for sg_root, sg_dirs, sg_files in os.walk(storage_dir, followlinks=True):
                # first check subdir for match
                for sg_dir in sg_dirs:
                    if self._match_title(sg_dir):
                        matched_dir_name = os.path.join(sg_root, sg_dir)
                        matched_storage_dir = storage_dir
                # check file names for match
                for sg_file in sg_files:
                    if self._match_title(sg_file):
                        logging.debug('Using storage dir with files matching title')
                        if sg_root == storage_dir:
                            return storage_dir, ''
                        return storage_dir, os.path.relpath(sg_root, storage_dir)
        # return directory matching title if found
        if matched_dir_name:
            logging.debug('Using storage dir matching title')
            return matched_storage_dir, os.path.relpath(matched_dir_name, matched_storage_dir)
        # return storage directory with max free space
        if max_space_storage_dir:
            logging.debug('Using storage dir with max. space')
            return max_space_storage_dir, ''
        return None, None

    def _build_name(self):
        """ Builds video file name: "The_title(_-_|_SxxEyy_][The_Subtitle].[mkv]" """
        parts = []
        title = self.recording.get_title()
        subtitle = self.recording.get_subtitle()
        season = self.recording.get_season()
        episode = self.recording.get_episode()
        if title and title != '':
            parts.append(title)
        if season > 0 and episode > 0:
            parts.append(f'S{season:02}E{episode:02}')
        elif subtitle and subtitle != "":
            parts.append('-')
        if subtitle and subtitle != "":
            parts.append(subtitle)
        name = "_".join(' '.join(parts).split()) + '.mkv'
        for char in ('\''):
            name = name.replace(char, '')
        return name

    def _match_title(self, name):
        """ Checks if file or directory name starts with specified title """
        simplified_title = self.recording.get_title().lower()
        simplified_name = name.lower()
        for char in (' ', '_', '-'):
            simplified_name = simplified_name.replace(char, '')
            simplified_title = simplified_title.replace(char, '')
        return simplified_name.startswith(simplified_title)


class Recording:
    """ Handles recording data """
    def __init__(self, rec_path):
        self.path = rec_path
        # first determine video stream of recording
        streams = Util.get_video_streams(self.path)
        self.video_stream = None
        for stream in streams:
            if 'codec_type' in stream and stream['codec_type'] == 'video':
                self.video_stream = stream
                break
        self.metadata = None

    def get_video_codec(self):
        """ Return video stream codec name """
        return self.video_stream['codec_name']

    def get_video_fps(self):
        """ Return video stream FPS """
        return float(self.video_stream['r_frame_rate'].split('/')[0])

    def get_uncut_list(self):
        """ Returns uncut parts of the recording """
        mbe = Backend()
        rec_id = mbe.get_recording_id(self.path)
        if rec_id is None:
            return None
        return mbe.get_recording_uncutlist(rec_id)

    def _get_metadata(self):
        if not self.metadata:
            mbe = Backend()
            rec_id = mbe.get_recording_id(self.path)
            if rec_id is None:
                return False
            self.metadata = mbe.get_recording_metadata(rec_id)

        return self.metadata is not None

    def get_title(self):
        """ Returns recording title """
        if self._get_metadata():
            return self.metadata['Title']
        return ''

    def get_subtitle(self):
        """ Returns recording subtitle """
        if self._get_metadata():
            return self.metadata['SubTitle']
        return ''

    def get_season(self):
        """ Returns recording season """
        if self._get_metadata():
            return int(self.metadata['Season'])
        return 0

    def get_episode(self):
        """ Returns recording episode """
        if self._get_metadata():
            return int(self.metadata['Episode'])
        return 0


class Transcoder:
    """ Handles transcoding a recording to a video file """
    def __init__(self, recording, preset, preset_file, timeout):
        self.timer = None
        self.recording = recording
        self.preset = preset
        self.preset_file = preset_file
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

    def transcode(self, dst_file):
        """ Transcode the source file to the destination file using the specified preset
            The cutlist of the recording (source file) is used to transcode
            multiple parts of the recording if neccessary and then merged into the final
            destination file.
        """
        # obtain recording parts to transcode
        parts = self.recording.get_uncut_list()
        if parts is None:
            return 1

        if parts:
            logging.debug('Found %s parts: %s', len(parts), parts)

        Status.init_progress()

        if not parts:
            # transcode whole file directly
            res = self._transcode_part(dst_file)
        elif len(parts) == 1:
            # transcode single part directly
            res = self._transcode_part(dst_file, parts[0])
        else:
            # transcode each part on its own
            res = self._transcode_multiple(dst_file, parts)

        Status.reset_progress()

        return res

    def copy(self, dst_file):
        """ Copies streams of the source file to the destination file using mkvmerge
            The cutlist of the recording (source file) is used to copy
            multiple parts of the recording if neccessary and then merged into the final
            destination file.
        """
        # obtain recording parts to transcode
        parts = self.recording.get_uncut_list()
        if parts is None:
            return 1

        if parts:
            logging.debug('Found %s parts: %s', len(parts), parts)

        Status.init_progress()

        res = self._copy_and_merge(dst_file, parts)

        Status.reset_progress()

        return res


    def extract(self, dst_file):
        """ Copies streams of the source file to the destination file using ffmpeg
            The cutlist of the recording (source file) is used to extract
            multiple parts of the recording if neccessary and then merged into the final
            destination file.
        """
        # obtain recording parts to extract
        parts = self.recording.get_uncut_list()
        if parts is None:
            return 1

        if parts:
            logging.debug('Found %s parts: %s', len(parts), parts)

        Status.init_progress()

        if not parts:
            # extract whole file directly
            res = self._extract_part(dst_file)
        elif len(parts) == 1:
            # extract single part directly
            res = self._extract_part(dst_file, parts[0])
        else:
            # extract each part on its own
            res = self._extract_multiple(dst_file, parts)

        Status.reset_progress()

        return res


    def _extract_multiple(self, dst_file, parts):
        # initialize progress ranges
        for part in parts:
            Status.add_subprogress(part[1]-part[0])

        # transcode each part on its own
        part_number = 1
        tmp_files = []
        dst_file_base_name, dst_file_ext = os.path.splitext(dst_file)
        for part in parts:
            dst_file_part = f'{dst_file_base_name}_part_{part_number}{dst_file_ext}'
            logging.info('Processing part %s/%s to %s', part_number, len(parts), dst_file_part)
            res = self._extract_part(dst_file_part, part)
            if res != 0:
                break
            part_number += 1
            tmp_files.append(dst_file_part)
            Status.next_subprogress()

        # merge transcoded parts
        if len(parts) == len(tmp_files):
            res = self._merge_parts(tmp_files, dst_file)

        # delete transcoded parts
        for tmp_file in tmp_files:
            Util.remove_file(tmp_file)

        return res


    def _transcode_multiple(self, dst_file, parts):
        # initialize progress ranges
        for part in parts:
            Status.add_subprogress(part[1]-part[0])

        # transcode each part on its own
        part_number = 1
        tmp_files = []
        dst_file_base_name, dst_file_ext = os.path.splitext(dst_file)
        for part in parts:
            dst_file_part = f'{dst_file_base_name}_part_{part_number}{dst_file_ext}'
            logging.info('Processing part %s/%s to %s', part_number, len(parts), dst_file_part)
            res = self._transcode_part(dst_file_part, part)
            if res != 0:
                break
            part_number += 1
            tmp_files.append(dst_file_part)
            Status.next_subprogress()

        # merge transcoded parts
        if len(parts) == len(tmp_files):
            res = self._merge_parts(tmp_files, dst_file)

        # delete transcoded parts
        for tmp_file in tmp_files:
            Util.remove_file(tmp_file)

        return res

    def _copy_and_merge(self, dst_file, parts):
        # start the copying process
        args = []
        args.append('mkvmerge')
        args.append('-o')
        args.append(dst_file)
        split_spec = ''
        for part in parts:
            split_spec += f'{"parts-frames:" if not split_spec else ",+"}{part[0]}-{part[1]}'
        if split_spec:
            args.append('--append-mode')
            args.append('track')
            args.append('--split')
            args.append(split_spec)
        args.append(self.recording.path)

        logging.debug('Executing \"%s\"', ' '.join(args))
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # start timer to abort transcode process if it hangs
        self._start_timer(proc)

        # regex pattern to find prograss and ETA in output line
        pattern = re.compile(r"^Progress:([ ]*[\d]*)")

        while True:
            line = proc.stdout.readline()
            if line == '' and proc.poll() is not None:
                break  # Aborted, no characters available, process died.
            if line:
                # new line, restart abort timer
                self._start_timer(proc)
                
                if line.startswith('Warning:'):
                    logging.warning(line)

                progress = None
                try:
                    if matched := re.search(pattern, line):
                        progress = float(matched.group(1))
                except IndexError:
                    pass
                else:
                    Status.set_progress(progress)
                # check if job was stopped externally
                if Status.canceled():
                    proc.kill()
                    break

        # remove video file on failure
        if proc.wait() == 2 or Status.canceled() or Status.failed():
            # print transcoding error output
            logging.error(proc.stderr.read())
            Util.remove_file(dst_file)

        self._stop_timer()

        if proc.returncode == 1:
            # a warning has occured
            logging.info('Finished with warning(s)')
            return 0

        return proc.returncode

    def _transcode_part(self, dst_file, frames=None):
        """ Start HandBrake to transcodes all or a single part (identified by
            start and end frame) of the source file
            A timer is used to abort the transcoding if there was no progress
            detected within a specfied timeout period.
        """

        # start the transcoding process
        args = []
        args.append('HandBrakeCLI')
        if self.preset_file:
            args.append('--presetfile')
            args.append(self.preset_file)
        args.append('--preset')
        args.append(self.preset)
        args.append('-i')
        args.append(self.recording.path)
        args.append('-o')
        args.append(dst_file)
        if frames:
            logging.debug('Processing frame %s to %s (%s frames)',
                          frames[0], frames[1], frames[1]-frames[0])
            # pass start and end position of remaining part to handbrake
            args.append('--start-at')
            args.append(f'frame:{frames[0]}')
            # stop it relative to start position
            args.append('--stop-at')
            args.append(f'frame:{frames[1]-frames[0]}')

        logging.debug('Executing \"%s\"', ' '.join(args))
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # start timer to abort transcode process if it hangs
        self._start_timer(proc)

        # regex pattern to find prograss and ETA in output line
        pattern = re.compile(r"([\d]*\.[\d]*)(?=\s\%.*fps)")

        while True:
            line = proc.stdout.readline()
            if line == '' and proc.poll() is not None:
                break  # Aborted, no characters available, process died.
            if line:
                # new line, restart abort timer
                self._start_timer(proc)

                progress = None
                try:
                    if matched := re.search(pattern, line):
                        progress = float(matched.group(1))
                except IndexError:
                    pass
                else:
                    Status.set_progress(progress)
                # check if job was stopped externally
                if Status.canceled():
                    proc.kill()
                    break

        # remove video file on failure
        if proc.wait() != 0 or Status.canceled() or Status.failed():
            # print transcoding error output
            logging.error(proc.stderr.read())
            Util.remove_file(dst_file)

        self._stop_timer()

        return proc.returncode

    def _extract_part(self, dst_file, frames=None):
        """ Use ffmpeg to copy video part. """
        if frames:
            fps = self.recording.get_video_fps()
            logging.debug('Using %s fps', fps)

        frame_count = float(frames[1]-frames[0]) if frames else 0

        streams = Util.get_video_streams(self.recording.path)

        # start the copying process
        args = []
        args.append('ffmpeg')
        if frames:
            args.append('-ss')
            args.append(f'{float(frames[0]) / fps}')
        args.append('-i')
        args.append(self.recording.path)
        if frames:
            args.append('-t')
            args.append(f'{float(frames[1]-frames[0]) / fps}')
        args.append('-vcodec')
        args.append('copy')
        args.append('-acodec')
        args.append('copy')
        args.append('-scodec')
        args.append('dvdsub')
        # select all video streams
        args.append('-map')
        args.append('0:v')
        # select all audio streams
        args.append('-map')
        args.append('0:a')
        sub_stream_index = 0
        for stream in streams:
            if not 'codec_type' in stream or not 'codec_name' in stream:
                continue
            if stream['codec_type'] != 'subtitle':
                continue
            if stream['codec_name'] == 'dvb_subtitle':
                args.append('-map')
                args.append(f'0:s:{sub_stream_index}')
            sub_stream_index += 1

        args.append(dst_file)
        logging.debug('Executing \"%s\"', ' '.join(args))
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # start timer to abort transcode process if it hangs
        self._start_timer(proc)

        # regex pattern to find prograss and ETA in output line
        pattern = re.compile(r"^frame=([ ]*[\d]*)")

        while True:
            line = proc.stderr.readline()
            if line == '' and proc.poll() is not None:
                break  # Aborted, no characters available, process died.
            if line:
                # new line, restart abort timer
                self._start_timer(proc)

                progress = None
                try:
                    if matched := re.search(pattern, line):
                        frame = float(matched.group(1))
                        progress = 100.0 * frame / frame_count
                except IndexError:
                    pass
                else:
                    Status.set_progress(progress)
                # check if job was stopped externally
                if Status.canceled():
                    proc.kill()
                    break

        # remove video file on failure
        if proc.wait() != 0 or Status.canceled() or Status.failed():
            # print transcoding error output
            logging.error(proc.stderr.read())
            Util.remove_file(dst_file)

        self._stop_timer()

        return proc.returncode

    @staticmethod
    def _merge_parts(parts, dst_file):
        logging.debug('Merging transcoded parts %s', parts)
        list_file = f'{os.path.splitext(dst_file)[0]}_partlist.txt'
        with open(list_file, "w") as text_file:
            for part in parts:
                text_file.write(f'file {os.path.basename(part)}\n')

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
        logging.debug('Executing \"%s\"', ' '.join(args))
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
    def __init__(self, debug=None):
        try:
            self.mbe = api.Send(host='localhost')
            result = self.mbe.send(
                endpoint='Myth/GetHostName'
            )
            self.host_name = result['String']
        except RuntimeError as error:
            logging.error('\nFatal error: "%s"', error)
        self.post_opts = {'wrmi': True}
        if debug is not None:
            self.post_opts['debug'] = debug
        elif logging.getLogger().getEffectiveLevel() == logging.DEBUG:
            self.post_opts['debug'] = True

    def get_storage_group_data(self, group_name=None):
        """ Retrieve storage group data from backend """
        if group_name:
            data = f'HostName={self.host_name}&GroupName={group_name}'
        else:
            data = f'HostName={self.host_name}'
        try:
            result = self.mbe.send(
                endpoint='Myth/GetStorageGroupDirs', rest=data
            )
            return result['StorageGroupDirList']['StorageGroupDirs']
        except RuntimeError as error:
            logging.error('\nFatal error: "%s"', error)
            return None

    def get_storage_dirs(self, group_name=None, host_name=None, writable=None):
        """ Returns list of storage group directories """
        storage_groups = self.get_storage_group_data(group_name)
        if not storage_groups:
            return []
        dirs = []
        for sg_data in storage_groups:
            if writable and sg_data["DirWrite"] != 'true':
                continue
            if not host_name or sg_data['HostName'] == host_name:
                dirs.append(sg_data['DirName'])
        return dirs

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
            return result['Program']
        except RuntimeError as error:
            logging.error('\nFatal error: "%s"', error)
        return None

    def get_recording_uncutlist(self, rec_id):
        """ Retrives cutlist of specified recording """
        try:
            data = f'RecordedId={rec_id}&OffsetType=Frames'
            result = self.mbe.send(
                endpoint='Dvr/GetRecordedCutList', rest=data
            )
            # create negated (uncut) list from cut list
            start = 0
            stop = 0
            cuts = []
            for cut in result['CutList']['Cuttings']:
                if int(cut['Mark']) == 1:  # start of cut
                    stop = int(cut['Offset'])
                    cuts.append((start, stop))
                elif int(cut['Mark']) == 0:  # end of cut
                    start = int(cut['Offset'])
                    stop = 9999999
            if stop == 9999999:
                cuts.append((start, stop))
            return cuts
        except RuntimeError as error:
            logging.error('\nFatal error: "%s"', error)
        return None

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
            return result['bool']
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

    def get_video_metadata(self, vid_id):
        """ Retrieves the metadata of the specified video file """
        try:
            data = f'Id={vid_id}'
            result = self.mbe.send(
                endpoint='Video/GetVideo', rest=data
            )
            return result['VideoMetadataInfo']
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
            return result['bool']
        except RuntimeError as error:
            logging.error('\nFatal error: "%s"', error)
        return False

    def show_notification(self, msg, msg_type):
        """ Displays a visual notification on active frontends """
        try:
            data = {
                'Message': msg,
                'Origin': '\"' + __file__ + '\"',
                'TimeOut': 60,
                'Type': msg_type,
                'Progress': -1
            }
            self.mbe.send(
                endpoint='Myth/SendNotification', postdata=data, opts=self.post_opts
            )
        except RuntimeError as error:
            logging.error('\nFatal error: "%s"', error)

        if msg_type == 'error':
            logging.error(msg)
        elif msg_type == "warning":
            logging.warning(msg)
        elif msg_type == "normal":
            logging.info(msg)


class Util:
    """ Utility class """
    @staticmethod
    def get_file_size(file_name):
        """ Return size of specified file """
        return os.stat(file_name).st_size

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
    def get_video_streams(filename):
        """ Determines all streams of the video file using ffprobe
            Returns list of streams.
        """
        args = []
        args.append('ffprobe')
        args.append('-hide_banner')
        args.append('-v')
        args.append('error')
        args.append('-show_streams')
        args.append('-of')
        args.append('json')
        args.append(filename)
        logging.debug('Executing \"%s\"', ' '.join(args))
        try:
            proc = subprocess.run(args, capture_output=True, text=True, check=True)
            return json.loads(proc.stdout)['streams']
        except subprocess.CalledProcessError as error:
            logging.error(error.stderr)
            return {}
        except ValueError:
            return {}

    @staticmethod
    def get_video_length(filename):
        """ Determines the video length using ffprobe
            Returns the video length in minutes.
        """
        streams = Util.get_video_streams(filename)
        if not streams:
            return 0
        for stream in streams:
            if 'codec_type' in stream and stream['codec_type'] == 'video':
                if 'duration' in stream:
                    return int(math.ceil(float(stream['duration']) / 60.0))
                if 'tags' in stream and 'DURATION' in stream['tags']:
                    tokens = stream['tags']['DURATION'].split(':')
                    if tokens:
                        return int(tokens[0]) * 60 + int(tokens[1])
                if 'tags' in stream and 'DURATION-eng' in stream['tags']:
                    tokens = stream['tags']['DURATION-eng'].split(':')
                    if tokens:
                        return int(tokens[0]) * 60 + int(tokens[1])
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
        director = []
        actors = []
        for member in rec_data['Cast']['CastMembers']:
            if member['Role'] == 'director':
                director.append(member['Name'])
            if member['Role'] == 'actor':
                actors.append(member['Name'])
        vid_length = Util.get_video_length(vid_path)
        fanart = None
        coverart = None
        for artwork in rec_data['Artwork']['ArtworkInfos']:
            if artwork['Type'] == 'coverart':
                coverart = os.path.split(artwork['FileName'])[1]
            elif artwork['Type'] == 'fanart':
                fanart = os.path.split(artwork['FileName'])[1]

        # update video metadata
        data = {}
        if 'Description' in rec_data:
            data['Plot'] = rec_data['Description']
        if 'Category' in rec_data:
            data['Genres'] = rec_data['Category'].title()
        if vid_length >= 1:
            data['Length'] = vid_length
        if director:
            data['Director'] = ', '.join(director)
        if actors:
            data['Cast'] = ', '.join(actors)
        if 'CatType' in rec_data and rec_data['CatType'] == 'movie':
            data['ContentType'] = 'MOVIE'
        if 'Airdate' in rec_data:
            data['Year'] = int(rec_data['Airdate'].split('-')[0])
        if fanart:
            data['Fanart'] = fanart
        if coverart:
            data['CoverFile'] = coverart

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

    @staticmethod
    def post_process(cmd, rec_path, vid_path):
        Status().set_comment('Post-Processing')
        cmd = cmd.replace('%VIDPATH%', vid_path)
        cmd = cmd.replace('%RECPATH%', rec_path)
        cmd = cmd.replace('%VIDFILE%', os.path.basename(vid_path))
        cmd = cmd.replace('%RECFILE%', os.path.basename(rec_path))
        args = shlex.split(cmd)
        try:
            logging.debug("Executing post-processing command: %s", args)
            proc = subprocess.run(args, capture_output=True, check=True, shell=False)
            logging.debug("Output:\n%s", proc.stdout.decode('utf-8').strip())
            logging.info("Exit code: %d", proc.returncode)
        except subprocess.CalledProcessError as error:
            logging.error(error.stderr.decode('utf-8'))
        except Exception as error:
            logging.error(error)


def parse_arguments():
    """ Parses command line arguments """
    parser = argparse.ArgumentParser(description='Convert recording and move it to video storage')
    parser.add_argument('-f', '--file', dest='rec_file', help='recording file name')
    parser.add_argument('-d', '--dir', dest='rec_dir', help='recording directory name')
    parser.add_argument('-p', '--path', dest='rec_path', help='recording path name')
    parser.add_argument('-j', '--jobid', dest='job_id', help='mythtv job id')
    parser.add_argument('-c', '--cfgfile', dest='cfg_file', default='~/rectovid.conf',
                        help='optional config file location (default: ~/rectovid.conf)')
    parser.add_argument('-m', '--mode', dest='mode',
                        help='Mode of processing (supported: copy, transcode) "copy" uses mkvmerge for stream copying, "transcode" uses Handbrake for transcoding')
    parser.add_argument('--preset', dest='preset',
                        help='Handbrake transcoding preset, call "HandBrakeCLI -z" to list supported presets')
    parser.add_argument('--presetfile', dest='preset_file',
                        help='Handbrake transcoding preset file to read from')
    parser.add_argument('--timeout', dest='timeout', type=int,
                        help='timeout in seconds to abort processing')
    parser.add_argument('--post-process', dest='post_command', help='post processing command')
    parser.add_argument('-l', '--logfile', dest='log_file', help='optional log file location, enables logging to file')
    parser.add_argument('--loglevel', dest='log_level',
                        help='optional log level (supported: debug, info, warning, error, critical; default: info)')

    args = parser.parse_args()

    # get options from config file if not passed as parameters
    config = configparser.ConfigParser()
    config.read(os.path.expanduser(args.cfg_file))
    if not args.mode:
        args.mode = config.get('General', 'Mode', fallback='copy')
    if not args.timeout:
        args.timeout = config.getint('General', 'Timeout', fallback=300)
    if not args.preset:
        args.preset = config.get('Transcoding', 'Preset', fallback='General/HQ 1080p30 Surround')
    if not args.preset_file:
        args.preset_file = config.get('Transcoding', 'PresetFile', fallback='')
    if not args.log_file:
        args.log_file = config.get('Logging', 'LogFile', fallback=None)
    if not args.log_level:
        args.log_level = config.get('Logging', 'LogLevel', fallback='info')
    if not args.post_command:
        args.post_command = config.get('Postprocessing', 'Command', fallback='')
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.log_level)

    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    logger = logging.getLogger()
    logger.setLevel(numeric_level)
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
    if args.log_file:
        log_handler = logging.handlers.RotatingFileHandler(os.path.expanduser(args.log_file), maxBytes=500*1024, backupCount=1)
        log_handler.setFormatter(formatter)
        logger.addHandler(log_handler)

    logging.debug('Options: %s', args)

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

    recording = Recording(rec_path)

    # build output file path
    vid_path = str(VideoFilePath(recording))
    if not vid_path:
        status.set_error('Could not find video storage directory or not enough free space available')
        sys.exit(2)
    if os.path.isfile(vid_path):
        status.set_error(f'Output video file already exists: \"{vid_path}\"')
        sys.exit(3)

    status.set_status(Job.RUNNING)

    # start processing
    logging.info('Started processing \"%s\"', recording.get_title())
    logging.info('Source recording file : %s', recording.path)
    logging.info('Destination video file: %s', vid_path)
    transcoder = Transcoder(recording, opts.preset, opts.preset_file, opts.timeout)
    if opts.mode.lower() == 'transcode':
        res = transcoder.transcode(vid_path)
    elif opts.mode.lower() == "copy":
        res = transcoder.copy(vid_path)
    elif opts.mode.lower() == "extract":
        res = transcoder.extract(vid_path)
    else:
        status.set_error(f'Unsupported processing mode: \"{args.mode}\"')
        sys.exit(4)

    if status.get_cmd() == Job.STOP:
        status.set_status(Job.CANCELLED)
        status.set_comment('Stopped processing')
        Util.show_notification(f'Stopped processing \"{recording.get_title()}\"', 'warning')
        sys.exit(5)
    elif res == 0:
        Util.add_video(recording.path, vid_path)
        Util.scan_videos()
    elif res != 0:
        status.set_error(f'Failed processing (error {res})')
        Util.show_notification(
            f'Failed processing \"{recording.get_title()}\" (error {res})', 'error'
        )
        sys.exit(res)

    if opts.post_command:
        Util.post_process(opts.post_command, recording.path, vid_path)

    rec_size = Util.format_file_size(Util.get_file_size(recording.path))
    vid_size = Util.format_file_size(Util.get_file_size(vid_path))
    size_status = f'{rec_size} => {vid_size}'
    Util.show_notification(
        f'Finished processing "{recording.get_title()}"\n{size_status}', 'normal'
    )
    status.set_comment(f'Finished processing\n{size_status}')
    status.set_status(Job.FINISHED)

    # .. the end
    sys.exit(0)


if __name__ == "__main__":
    main()
