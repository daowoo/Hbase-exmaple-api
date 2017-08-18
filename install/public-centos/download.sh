#!/usr/bin/env bash

# timezone config
echo 'modify lang and timezone'
sudo localectl set-locale LANG=en_US.utf8
sudo timedatectl set-timezone Asia/Shanghai

# copy vagrant ssh config
sudo mkdir -p /root/.ssh; 
sudo chmod 600 /root/.ssh; 
sudo cp /home/vagrant/.ssh/authorized_keys /root/.ssh/

# create install
sudo cat << eof > ~/install.sh
#!/bin/bash

curl -O http://192.168.36.118/resource/initenv.sh
chmod +x initenv.sh
./initenv.sh

curl -O http://192.168.36.118/resource/inithadoop.sh
chmod +x inithadoop.sh
./inithadoop.sh

curl -O http://192.168.36.118/resource/initzookeeper.sh
chmod +x initzookeeper.sh
./initzookeeper.sh

curl -O http://192.168.36.118/resource/inithbase.sh
chmod +x inithbase.sh
./inithbase.sh
eof

. ~/install.sh

echo "Environment initialization completed!"