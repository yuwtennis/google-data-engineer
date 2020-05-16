#!/bin/bash

USER_DATA_DIR=/tmp/junk

rm -rf ${USER_DATA_DIR}
google-chrome-stable --proxy-server='socks5://localhost:1080' --host-resolver-rules="MAP * 0.0.0.0, EXCLUDE localhost" --user-data-dir=${USER_DATA_DIR}
