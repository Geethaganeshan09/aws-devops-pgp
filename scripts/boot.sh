#!/bin/sh
sudo rm -r /home/ubuntu/aws-devops-pgp
kill $(ps aux | grep python | grep app.py | awk '{print $2}')
