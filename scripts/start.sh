cd /home/ubuntu/pgp-june
sudo pip3 install pymysql flask boto3
sudo touch log.txt
sudo chmod 777 log.txt
sudo nohup python3 app.py > log.txt 2>&1 &
