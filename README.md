# mythlets
Scripts for Mythtv

## rectovid.py

Supports transcoding of a recording using HandBrakeCLI or cutting and merging a recording using mkvmerge.
The resulting file is moved to a MythTv video storage group.
This script can be set as a user job and can then be triggered from MythFrontend on a specific recording.

    usage: rectovid.py [-h] [-f REC_FILE] [-d REC_DIR] [-p REC_PATH] [-j JOB_ID] [-c CFG_FILE] [-m MODE] [--preset PRESET]
                    [--presetfile PRESET_FILE] [--timeout TIMEOUT] [-l LOG_FILE] [--loglevel LOG_LEVEL]

    Convert recording and move it to video storage

    optional arguments:
    -h, --help            show this help message and exit
    -f REC_FILE, --file REC_FILE
                            recording file name
    -d REC_DIR, --dir REC_DIR
                            recording directory name
    -p REC_PATH, --path REC_PATH
                            recording path name
    -j JOB_ID, --jobid JOB_ID
                            mythtv job id
    -c CFG_FILE, --cfgfile CFG_FILE
                            optional config file location (default: ~/rectovid.conf)
    -m MODE, --mode MODE  Mode of processing (supported: copy, transcode) "copy" uses mkvmerge for stream copying, "transcode" uses
                            Handbrake for transcoding
    --preset PRESET       Handbrake transcoding preset, call "HandBrakeCLI -z" to list supported presets
    --presetfile PRESET_FILE
                            Handbrake transcoding preset file to read from
    --timeout TIMEOUT     timeout in seconds to abort processing
    -l LOG_FILE, --logfile LOG_FILE
                            optional log file location, enables logging to file
    --loglevel LOG_LEVEL  optional log level (supported: debug, info, warning, error, critical; default: info)

## time_since_last_boot.sh

Small script to print the time sine the last boot of the system.

    Usage: ./time_since_last_boot.sh [-s] [-m]

    Print time since last boot

    Options:
        -s  Print seconds sind last boot to stdout
        -m  Print minutes sind last boot to stdout
        -h  Print this help

## parsecppcheck.py
Parses dumpfile of cppcheck

## themestatus.py
Parses theme xml files and mythtv source file to determine theme progress/completeness

## mythtv-set-wakeup
Set acpi wakeup time for scheduled recording

## mythtv-pre-shutdown
Checks if shutdown of mythbackend is safe

## lirc-set-protocol
Sets IR protocol to lirc
