#!/bin/bash
set -e
set -x

export QT_X11_NO_MITSHM=1
export _X11_NO_MITSHM=1
export _MITSHM=0

# In case the container is restarted 
[ -f /tmp/.X99-lock ] && rm /tmp/.X99-lock


_kill_procs() {
  kill -TERM $pyppeteer
  kill -TERM $xvfb
}

# Relay quit commands to processes
trap _kill_procs SIGTERM SIGINT

export DISPLAY=:0
Xvfb $DISPLAY -screen 0 1024x768x16 -nolisten tcp -nolisten unix &
xvfb=$!

# https://linux.die.net/man/1/x11vnc
x11vnc -nopw -display $DISPLAY -N -forever > /dev/null &
x11vnc=$!

if [[ "${HEADLESS}" == "true" ]]; then
  HEADLESS_FLAG="--headless"
else
  HEADLESS_FLAG=""
fi

if [[ -z $URL ]]; then
  # Start HTTP service
  python3 ./src/service.py $HEADLESS_FLAG &
else
  # Start chrome
  python3 ./src/process_url.py --url $URL --report_type $REPORT_TYPE --request_id $REQUEST_ID --timeout $TIMEOUT $HEADLESS_FLAG &
fi
pyppeteer=$!

wait $pyppeteer
# wait $xvfb
# wait $x11vnc