#!/bin/bash
cd /mnt/Storage1/Containers/sap-reporting || exit
git fetch origin
git reset --hard origin/main
sudo docker compose up -d --build
