#!/bin/bash
cd /mnt/Storage1/Containers/sap-reporting || exit
git add .
git commit -m "update"
git push origin main

