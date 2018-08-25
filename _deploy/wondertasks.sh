#!/bin/bash
set -euo pipefail
set -x

# -- configuration -------------------------------------------------------------
username=andreyvit
internal_http_port=3033

# -- parse options -------------------------------------------------------------
deploy=false
reconfigure=false
while test "$#" -gt 0; do
    case "$1" in
        --deploy)      deploy=true;      shift;;
        --reconfigure) reconfigure=true; shift;;
        *) echo "** invalid option '$1'"; exit 1;;
    esac
done

# -- directories ---------------------------------------------------------------
sudo install -d -m755 -g$username -o$username /srv/wondertasks/{bin,db}

# -- deploy code ---------------------------------------------------------------
if $deploy; then
    sudo install -m755 -g$username -o$username wondertasks-linux-amd64 /srv/wondertasks/bin/wondertasks
fi

if $reconfigure; then

# -- configure daemon ----------------------------------------------------------
sudo install -m644 -groot -oroot /dev/stdin /etc/systemd/system/wondertasks.service <<EOF
[Unit]
Description=Wonderland Task Sync Server
After=network.target

[Service]
User=$username
Restart=always
PIDFile=/var/run/wondertasks.pid
Type=simple
ExecStart=/srv/wondertasks/bin/wondertasks -dev -addr localhost:$internal_http_port -db /srv/wondertasks/db
KillMode=process

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable wondertasks

# -- configure Caddy -----------------------------------------------------------
sudo install -m644 -groot -oroot /dev/stdin /srv/wondertasks/Caddyfile <<EOF
sync.todo.softwarewonderland.com {
    proxy / localhost:$internal_http_port {
        transparent
    }
    tls andrey@tarantsov.com
}
EOF

sudo systemctl restart caddy

fi

# -- start ---------------------------------------------------------------------
sudo systemctl restart wondertasks
