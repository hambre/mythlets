#!/usr/bin/env python

import argparse, sys, os

sys.path.append("/usr/bin")

def FindDirByTitle(dirName, title):
    for root, dirs, files in os.walk(dirName):
        for name in files:
            index = name.find(title)
            if index == 0:
                return root
    # return initial directory
    return dirName 

def ShowNotification(msgText, msgType):
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

def main():
    parser = argparse.ArgumentParser(description='Transcoding recording and move to videos')
    parser.add_argument('-f', '--file', dest='recFile', help='recording file name')
    parser.add_argument('-d', '--dir', dest='recDir', help='recording directory name')
    parser.add_argument('-p', '--path', dest='recPath', help='recording path name')
    parser.add_argument('-t', '--title', dest='recTitle', help='recording title')
    parser.add_argument('-s', '--subtitle', dest='recSubtitle', help='recording subtitle')
    parser.add_argument('-sn', '--season', dest='recSeason', default=0, type=int, help='recording season number')
    parser.add_argument('-en', '--episode', dest='recEpisode', default=0, type=int, help='recording episode number')
    opts = parser.parse_args()
    
    recPath = None
    if opts.recPath:
        recPath = opts.recPath
    elif opts.recDir and opts.recFile:
        recPath = os.path.join(opts.recDir, opts.recFile)
    if not recPath:
        sys.stderr.write('Please specify a recording path or recording directoy and recording file\n')
        sys.exit(1)

    if opts.recTitle == None and opts.recSubtitle == None:
        sys.stderr.write('Please specify a title and/or subtitle\n')
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
    vidFile = "_".join(' '.join(parts).split()) + ".m4v"

    # build output file path
    # TODO pull video storage dir(s) from backend
    vidDir = FindDirByTitle("/srv/mythtv/media1/movies/", "_".join(opts.recTitle.split()))
    vidPath = os.path.join(vidDir, vidFile)
    if not os.path.isfile(recPath):
        sys.stderr.write('Input recording file does not exist\n')
        sys.exit(2)
    if os.path.isfile(vidPath):
        sys.stderr.write('Output video file already exists\n')
        sys.exit(3)

    # start transcoding 
    # TODO use subprocess for async processing and progress reporting
    args = []
    args.append('HandBrakeCLI')
    args.append('--preset')
    args.append('HQ 1080p30 Surround')
    args.append('-i')
    args.append(recPath)
    args.append('-o')
    args.append(vidPath)
    res = os.spawnvp(os.P_WAIT, 'HandBrakeCLI', args)
    if res != 0:
        ShowNotification('Failed transcoding \"{}\" (error {})'.format(opts.recTitle, res), 'error')
        sys.exit(res)
        
    ShowNotification('Finished transcoding \"{}\"'.format(opts.recTitle), 'normal')
    
    # scan videos
    args = []
    args.append('mythutil')
    args.append('--scanvideos')
    res = os.spawnvp(os.P_WAIT, 'mythutil', args)

    # .. the end
    sys.exit(0)

if __name__ == "__main__":
    main()
