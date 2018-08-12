#!/bin/bash
set -euo pipefail
sudo mkdir -p /srv/wonderlandtasks/db
sudo chown -R andreyvit:andreyvit /srv/wonderlandtasks/db
sudo install -m644 -groot -oroot -p ~/wondertaskserver.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable wondertaskserver
sudo systemctl restart wondertaskserver
