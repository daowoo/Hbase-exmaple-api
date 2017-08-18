#!/usr/bin/env bash

LOC_DOMAIN="panhongfa.com"
NTP_SERVER_NAME="server"
REPO_SERVER_NAME="server"
DNS_SERVER_NAME="server"
DNS_SERVER_IP="192.168.36.118"

ntpserver=${NTP_SERVER_NAME}.${LOC_DOMAIN}
reposerver=${REPO_SERVER_NAME}.${LOC_DOMAIN}
dnsserver=${DNS_SERVER_NAME}.${LOC_DOMAIN}

#hosts config
echo 'modify hosts config'
sudo cat << eof > /etc/hosts
127.0.0.1   localhost localhost.localdomain localhost4 localhost4.localdomain4
::1         localhost localhost.localdomain localhost6 localhost6.localdomain6
eof

#timezone config
echo 'modify lang and timezone'
sudo localectl set-locale LANG=en_US.utf8
sudo timedatectl set-timezone Asia/Shanghai

#dns config
echo 'modify dns config'
sudo cat << eof > /etc/NetworkManager/NetworkManager.conf
[main]
plugins=ifcfg-rh
dns=none

[logging]
#level=DEBUG
#domains=ALL
eof

sudo systemctl restart NetworkManager.service
sudo cat << eof > /etc/resolv.conf
# Generated by NetworkManager
search ${LOC_DOMAIN}
nameserver ${DNS_SERVER_IP}
eof

#repo config
echo 'modify repo config'
sudo mv /etc/yum.repos.d /etc/yum.repos.d.bak
sudo mkdir /etc/yum.repos.d
sudo curl -o /etc/yum.repos.d/local.repo http://${reposerver}/resource/local.repo
sudo curl -o /etc/yum.repos.d/CentOS7-Base-aliyun.repo http://${reposerver}/resource/CentOS7-Base-aliyun.repo

#yum cache
echo 'update yum cache'
sudo yum clean all
sudo yum repolist

#install tools
echo 'install all tools'
sudo yum install tree -y
sudo yum install wget -y
sudo yum install yum-utils -y
sudo yum install bash-completion -y

#stop firewalld
echo 'stop firewalld and selinux'
sudo systemctl stop firewalld.service
sudo systemctl disable firewalld.service
setenforce 0
sudo sed -i 's/SELINUX=.*/SELINUX=disabled/' /etc/selinux/config

#ntp config
echo 'ntp client config'
sudo yum install ntp -y
sudo sed -i 's/server [0-3].centos.*/server ${ntpserver}/' /etc/ntp.conf
sudo systemctl start ntpd.service
sudo systemctl enable ntpd.service
sudo timedatectl set-ntp yes

ntpdate -u ${ntpserver}
ntpq -p

#mount nfs
echo 'mount nfs disk'
sudo mkdir -p /mnt/nfs
sudo yum install nfs-utils -y
#mount -v -t nfs 192.168.80.73:/nfsios /mnt/nfs

#conflict resolution
echo 'conflict resolution'
sudo yum erase -y snappy.x86_64
sudo systemctl stop chronyd.service
sudo systemctl disable chronyd.service
sudo systemctl restart ntpd.service

#Increasing swap space
echo 'swap config'
sudo dd if=/dev/zero of=/swapfile bs=1024 count=1024k
sudo mkswap /swapfile
sudo swapon /swapfile
echo "/swapfile       none    swap    sw      0       0" >> /etc/fstab

#root ssh login config
echo 'ssh login config'
wget -N -P /tmp/ http://${reposerver}/resource/ssh.tar.gz
tar -zxvf /tmp/ssh.tar.gz -C /tmp/

sudo mkdir -p /root/.ssh
sudo cp -f /tmp/ssh/root/id_rsa /tmp/ssh/root/id_rsa.pub /root/.ssh/
sudo cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys
sudo chmod 600 /root/.ssh/*

#add user and group
sudo groupadd hadoop
sudo useradd -d /home/hadoop -g hadoop hadoop

#install JDK8
wget -N -P /home/hadoop/ http://server.panhongfa.com/resource/jdk-8u112-linux-x64.tar.gz
mkdir /home/hadoop/java
tar -zxf /home/hadoop/jdk-8u112-linux-x64.tar.gz -C /home/hadoop/java/

java_var=$(sed -n '/^#JAVA_HOME$/'p /etc/profile)
if [ ! $java_var ]; then
  echo "JAVA_HOME UNSET"
  cat << 'eof' >> /etc/profile
#JAVA_HOME
export JAVA_HOME=/home/hadoop/java/jdk1.8.0_112
export JRE_HOME=$JAVA_HOME/jre

export CLASSPATH=.:$JAVA_HOME/lib:$CLASSPATH
export PATH=$JAVA_HOME/bin:$PATH
eof
source /etc/profile
else
  echo "JAVA_HOME EXIST"
fi

#hadoop ssh login config
sudo mkdir -p /home/hadoop/.ssh
sudo cp -f /tmp/ssh/hadoop/id_rsa /tmp/ssh/hadoop/id_rsa.pub /home/hadoop/.ssh/
sudo cat /home/hadoop/.ssh/id_rsa.pub >> /home/hadoop/.ssh/authorized_keys

sudo chown -R hadoop:hadoop /home/hadoop
sudo chmod -R 700 /home/hadoop
sudo chmod 600 /home/hadoop/.ssh/*

#hostname, ipaddr and add disk
wget -N -P /tmp/ http://${reposerver}/resource/script.tar.gz
tar -zxvf /tmp/script.tar.gz -C /tmp/
#. /tmp/script/hostname_set.sh
#. /tmp/script/ipaddr_set.sh
#. /tmp/script/add_disk.sh

#network restart
sudo service network restart