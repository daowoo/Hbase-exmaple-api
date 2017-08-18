#!/usr/bin/env bash

cp /vagrant/hosts /etc/hosts

# copy vagrant ssh config
sudo mkdir -p /root/.ssh;
sudo chmod 600 /root/.ssh;
sudo cp /home/vagrant/.ssh/authorized_keys /root/.ssh/

# disable firewalld
sudo systemctl stop firewalld.service
sudo systemctl disable firewalld.service

setenforce 0
sudo sed -i 's/SELINUX=.*/SELINUX=disabled/' /etc/selinux/config

# modify remote repo
sudo mv /etc/yum.repos.d /etc/yum.repos.d.bak
sudo mkdir /etc/yum.repos.d
sudo cp /vagrant/repo/CentOS7-Base-aliyun.repo /etc/yum.repos.d/CentOS7-Base-aliyun.repo

# install tools
sudo yum repolist
sudo yum install tree -y
sudo yum install wget -y
sudo yum install yum-utils -y
sudo yum install bash-completion -y

# install httpd server
sudo mkdir /home/repo
sudo yum install httpd -y

# config repo web server
sudo cat << eof > /etc/httpd/conf.d/local_repo.conf
<VirtualHost *:80>
  DocumentRoot "/home/repo"
  <Directory "/home/repo">
    Options Indexes FollowSymLinks
    AllowOverride None
    Require all granted
  </Directory>
</VirtualHost>
eof

sudo sed -i '/^IncludeOptional/s/^[^#]/#&/' /etc/httpd/conf/httpd.conf
echo "IncludeOptional conf.d/local_repo.conf" >> /etc/httpd/conf/httpd.conf
echo "IncludeOptional conf.d/autoindex.conf" >> /etc/httpd/conf/httpd.conf


# start httpd.service
sudo systemctl restart httpd.service
sudo systemctl enable httpd.service

# install ntp server
sudo yum install ntp -y
sudo ntpdate time.ntp.org
sudo timedatectl set-ntp yes

# try stop chronyd service
sudo systemctl disable chronyd.service
sudo systemctl stop chronyd.service

# config timezone
sudo localectl set-locale LANG=en_US.utf8
sudo timedatectl set-timezone Asia/Shanghai

# config ntp server
sudo sed -i '/^#restrict.*notrap$/a restrict 192.168.0.0 mask 255.255.0.0 nomodify notrap' /etc/ntp.conf
sudo sed -i '/server 127.127.1.0/d' /etc/ntp.conf
sudo sed -i '/fudge 127.127.1.0 stratum 10/d' /etc/ntp.conf
echo "server 127.127.1.0" >> /etc/ntp.conf
echo "fudge 127.127.1.0 stratum 10" >> /etc/ntp.conf

# start ntp service
sudo systemctl restart ntpd.service
sudo systemctl enable ntpd.service

# install named server
sudo yum install -y bind
sudo yum install -y bind-utils

# Increasing swap space
sudo dd if=/dev/zero of=/swapfile bs=1024 count=1024k
sudo mkswap /swapfile
sudo swapon /swapfile
echo "/swapfile       none    swap    sw      0       0" >> /etc/fstab

sudo service network restart
