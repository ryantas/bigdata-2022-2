# mongo = 4.4.1 < 6.1

conda activate bigdata-2022-2
conda rm mongodb mongo-tools
sudo service mongod stop
sudo apt-get purge mongodb-org*
sudo rm -r /var/log/mongodb
sudo rm -r /var/lib/mongodb

# esto es para ubuntu 18.04, para los que tiene 20.04 (https://computingforgeeks.com/how-to-install-latest-mongodb-on-ubuntu/)

echo "deb [ arch=amd64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.4 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb.list
sudo apt update
sudo apt install mongodb-org=4.4.1 mongodb-org-server=4.4.1 mongodb-org-shell=4.4.1 mongodb-org-mongos=4.4.1 mongodb-org-tools=4.4.1

sudo rm -rf /tmp/mongodb-27017.sock
sudo service mongod start
