#!/bin/bash

sleep 30
/opt/mycenae/bin/mycenae -config=/etc/opt/mycenae/mycenae.toml 2>&1 | cronolog -S /var/log/mycenae/mycenae.log "/var/log/mycenae/mycenae.log-%Y-%m-%d_%H:%M:%S" &
