#!/bin/sh
sudo rm -r /home/ubuntu/pgp-june
kill $(ps aux | grep python | grep app.py | awk '{print $2}')
