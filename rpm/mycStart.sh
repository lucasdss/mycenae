#!/bin/bash

project="mycenae"
process=$(/usr/bin/pgrep $project |wc -l)

if [ $process -gt 0 ]
then
     echo "$project is already running!"
     exit 1
fi

ulimit -n 99999
ulimit -l unlimited
/opt/mycenae/bin/mycenae -config=/etc/opt/mycenae/mycenae.toml 2>&1 | cronolog -S /var/log/mycenae/mycenae.log "/var/log/mycenae/mycenae.log-%Y-%m-%d" &
