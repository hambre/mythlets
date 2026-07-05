#!/bin/bash

usage() {
    echo "Usage: $0 [-s] [-m] [-f 'seconds' | 'minutes:ss' | 'hours:mm:ss' ]"
    echo ""
    echo "Print time since last boot"
    echo ""
    echo "Options:"
    echo "  -s  Print seconds sind last boot to stdout"
    echo "  -m  Print minutes sind last boot to stdout"
    echo "  -f  Exit with error when last timespan since last reboot exceeds given value"
    echo "  -h  Print this help"
}

time_since_last_boot_in_seconds() {
  LAST_BOOT_DATE=$(last -x -F -T reboot | head -2 | grep -v "still running" | cut -f 4)

  BOOT_SECONDS=$(date -d "${LAST_BOOT_DATE}" +%s)
  CURR_SECONDS=$(date +%s)

  echo "$(( ${CURR_SECONDS} - ${BOOT_SECONDS} ))"
}

timespan_to_seconds() {
    local F3=$(echo "$1" | cut -d ':' -f 3)
    local F2=$(echo "$1" | cut -d ':' -f 2)
    local F1=$(echo "$1" | cut -d ':' -f 1)

    local SECONDS=0
    if [[ "$1" =~ ^([0-9]{1,}):([0-2][0-3]|[0-9]):([0-5][0-9]|[0-9])$ ]]; then # "hours:mm:ss"
        SECONDS=$(( $F1 * 60 * 60 + $F2 * 60 + $F3 ))
    elif [[ "$1" =~ ^([0-9]{1,}):([0-5][0-9]|[0-9])$ ]]; then # "minutes:ss"
        SECONDS=$(( $F1 * 60 + $F2 ))
    elif [[ "$1" =~ ^[0-9]{1,}$ ]]; then # "seconds"
        SECONDS=$F1
    else
        echo "Failed to parse timespan $1"
        usage
        return
    fi
    TIME_DIFF_TO_FAIL=${SECONDS}
}

TIME_DIFF_SEC=$(time_since_last_boot_in_seconds)
TIME_DIFF_MIN=$(( ${TIME_DIFF_SEC} / 60 ))

while getopts ":smf:h" opt; do
    case $opt in
        s) SILENT=1; echo "${TIME_DIFF_SEC}" ;;
        m) SILENT=1; echo "${TIME_DIFF_MIN}" ;;
        f) SILENT=1; timespan_to_seconds $OPTARG ;;
        h) usage; exit 0 ;;
    esac
done
shift $((OPTIND - 1))

if [ -z ${SILENT+x} ]; then
    echo "Seconds since last boot: ${TIME_DIFF_SEC}"
    echo "Minutes since last boot: ${TIME_DIFF_MIN}"
fi

if [ ! -z ${TIME_DIFF_TO_FAIL+x} ]; then
    if [ ${TIME_DIFF_TO_FAIL} -gt 0 ] && [ ${TIME_DIFF_TO_FAIL} -lt ${TIME_DIFF_SEC} ]; then
        exit 1
    fi
fi