#!/usr/bin/env python2

import argparse, sys, os, subprocess
from MythTV import Job
from MythTV.database import DBCache

sys.path.append("/usr/bin")

def decodeName(name):
    if type(name) == str: # leave unicode ones alone
        try:
            name = name.decode('utf8')
        except:
            name = name.decode('windows-1252')
    return name

def getFreeSpace(filename):
    stats = os.statvfs(filename)
    return stats.f_bfree * stats.f_frsize

# find storage directory by recording title
def matchTitle(title, name):
    t = title.lower()
    n = name.lower()
    for c in (' ', '_', '-'):
        n = n.replace(c, '')
        t = t.replace(c, '')
    return n.startswith(t)

# Uses the following criteria by descending priority
# 1. Storage dir with maximum free space
# 2. Directory matching recording title (useful for series)
# 3. Directory containing files matching the title
def findStorageDirByTitle(title):
    db = DBCache(None)
    matchDirName = None
    title = decodeName(title)
    maxFreeSpace = 0
    maxFreeDirName = None
    for sg in db.getStorageGroup(groupname='Videos'):
        # search given group
        if sg.local and os.path.isdir(sg.dirname):
            # get avaliable space of storage group partition
            # and use storage group with max. available space
            freeSpace = getFreeSpace(sg.dirname)
            if freeSpace > maxFreeSpace:
                maxFreeDirName = sg.dirname
                maxFreeSpace = freeSpace
            for root, dirs, files in os.walk(sg.dirname):
                # first check subdir for match
                for d in dirs:
                    if matchTitle(title, decodeName(d)):
                        matchDirName = os.path.join(root, d)
                # check file names for match
                for f in files:
                    if matchTitle(title, decodeName(f)):
                        return root
    # return directory matching title if found
    if matchDirName:
        return matchDirName
    # return storage directory with max free space
    return maxFreeDirName 

def formatFileSize(num):
    for unit in ['B','KB','MB','GB','TB']:
        if abs(num) < 1000.0:
            return "%3.1f %s" % (num, unit)
        num /= 1000.0
    return "%.1f %s" % (num, 'PB')

def showNotification(msgText, msgType):
    args= []
    args.append('mythutil')
    args.append('--notification')
    args.append('--origin')
    args.append('\"' + __file__ +'\"')
    args.append('--timeout')
    args.append('60')
    args.append('--message_text')
    args.append(msgText)
    args.append('--type')
    args.append(msgType)
    res = os.spawnvp(os.P_WAIT, 'mythutil', args)
    if msgType == 'error':
        sys.stderr.write(msgText + '\n')

def logError(mythJob, errorMsg):
    if mythJob:
        mythJob.setComment(errorMsg)
    sys.stderr.write(errorMsg + '\n')

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
    opts = parser.parse_args()
    
    mythJob = None
    if opts.jobId:
        mythJob = Job(opts.jobId)

    recPath = None
    if opts.recPath:
        recPath = opts.recPath
    elif opts.recDir and opts.recFile:
        recPath = os.path.join(opts.recDir, opts.recFile)
    if not recPath:
        logError(mythJob, 'Recording path or recording directoy + recording file not specified')
        sys.exit(1)

    if opts.recTitle == None and opts.recSubtitle == None:
        logError(mythJob, 'Title and/or subtitle not specified')
        sys.exit(1)
    
    # build output file name
    parts = []
    if opts.recTitle and opts.recTitle != "":
        parts.append(opts.recTitle)
    if opts.recSeason > 0 and opts.recEpisode > 0:
        parts.append("S{:0>2}E{:0>2}".format(opts.recSeason, opts.recEpisode))
    elif opts.recSubtitle and opts.recSubtitle != "":
        parts.append('-')
    if opts.recSubtitle and opts.recSubtitle != "":
        parts.append(opts.recSubtitle)
    vidFile = decodeName("_".join(' '.join(parts).split()) + ".m4v")

    # build output file path
    vidDir = findStorageDirByTitle("_".join(opts.recTitle.split()))
    if not vidDir:
        logError(mythJob, 'Could not find video storage directory')
        sys.exit(2)
    vidPath = os.path.join(vidDir, vidFile)
    if not os.path.isfile(recPath):
        logError(mythJob, 'Input recording file does not exist')
        sys.exit(3)
    if os.path.isfile(vidPath):
        logError(mythJob, 'Output video file already exists')
        sys.exit(4)

    if mythJob:
        mythJob.update(status=Job.STARTING)
        mythJob.setStatus(Job.RUNNING)

    # start transcoding 
    args = []
    args.append('HandBrakeCLI')
    args.append('--preset')
    args.append('HQ 1080p30 Surround')
    args.append('-i')
    args.append(recPath)
    args.append('-o')
    args.append(vidPath)
    res = 0
    if mythJob:
        cp = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
                    #print('Progress: {} Remaining time: {}'.format(progress, eta))
                    mythJob.setComment('Progress: {} %\nRemaining time: {}'.format(progress, eta))
                    lastProgress = int(float(progress))
                line = ''
            else:
                line += nl
        res = cp.wait()
    else:
        res = os.spawnvp(os.P_WAIT, 'HandBrakeCLI', args)

    if res != 0:
        if os.isfile(vidPath):
            os.remove(vidPath)
        logError(mythJob, 'Failed transcoding (error {})'.format(res))
        showNotification('Failed transcoding \"{}\" (error {})'.format(opts.recTitle, res), 'error')
        sys.exit(res)

    recSize = os.stat(recPath).st_size
    vidSize = os.stat(vidPath).st_size
    sizeStatus = formatFileSize(recSize) + ' => ' + formatFileSize(vidSize)
    
    showNotification('Finished transcoding \"{}\"'.format(opts.recTitle) + '\n' + sizeStatus, 'normal')
    
    if mythJob:
        mythJob.setComment('Triggering video rescan')

    # scan videos
    args = []
    args.append('mythutil')
    args.append('--scanvideos')
    res = os.spawnvp(os.P_WAIT, 'mythutil', args)

    if mythJob:
        mythJob.setComment('Finished transcoding\n' + sizeStatus)
        mythJob.setStatus(Job.FINISHED)

    # .. the end
    sys.exit(0)

if __name__ == "__main__":
    main()

