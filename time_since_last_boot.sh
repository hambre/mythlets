#!/bin/bash

usage() {
    echo "Print time since last boot"
    echo ""
    echo "Usage: $0 [-s] [-m]"
    echo ""
    echo "Options:"
    echo "  -s  Print seconds sind last boot to stdout"
    echo "  -m  Print minutes sind last boot to stdout"
    echo "  -h  Print this help"
}

time_since_last_boot_in_seconds() {
  LAST_BOOT_DATE=$(last -x -F -T reboot | head -2 | grep -v "still running" | cut -f 4)

  BOOT_SECONDS=$(date -d "${LAST_BOOT_DATE}" +%s)
  CURR_SECONDS=$(date +%s)

  echo "$(( ${CURR_SECONDS} - ${BOOT_SECONDS} ))"
}

TIME_DIFF_SEC=$(time_since_last_boot_in_seconds)
TIME_DIFF_MIN=$(( ${TIME_DIFF_SEC} / 60 ))

while getopts ":smh" opt; do
    case $opt in
        s) SILENT=1; echo "${TIME_DIFF_SEC}" ;;
        m) SILENT=1; echo "${TIME_DIFF_MIN}" ;;
        h) usage; exit 0 ;;
    esac
done
shift $((OPTIND - 1))

if [ -z ${SILENT+x} ]; then
    echo "Seconds since last boot: ${TIME_DIFF_SEC}"
    echo "Minutes since last boot: ${TIME_DIFF_MIN}"
fi
