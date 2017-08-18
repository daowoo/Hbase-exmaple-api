# Ambari demo 安装演示

软件要求： `vagrant`

硬件： `16g memory`

FTP下载centos7 镜像  `/share/DataScience/CentOS-7-x86_64-Vagrant-1611_01.VirtualBox.box`
将该镜像加入到vagrant 仓库

```
>>vagrant box add centos/7 ./CentOS-7-x86_64-Vagrant-1611_01.VirtualBox.box
```

完成以后应该能看到下面的输出：
```
>>cye@cye-MS-2016:~$ vagrant box list
centos/7  (virtualbox, 0)
```

Clone 演示代码： `git clone git@192.168.30.6:yechangqing/ambari-demo.git`

进入ambari-demo文件夹,启动ambari server
```
>>cd ambari-demo
```
FTP下载jdk安装包(`/share/DataScience/jdk-8u77-linux-x64.tar.gz`)到当前目录，ambari server需要把这个package分发到所有host上。

启动ambari server, ssh 进入启动的虚拟机 
```
>>vagrant up server
>>vagrant ssh server
```

在server虚拟机上运行
```

>>sudo su -  //换成root

//加入自建的ambari repo
>>sh -c "echo ' 
[hdp] 
name = this is a local repository 
baseurl = http://192.168.30.234/repos/hdp/HDP/centos7 
gpgcheck = 0 
enabled = 1 
cost=1 

[hdp_util] 
name = this is a local repository 
baseurl = http://192.168.30.234/repos/hdp/HDP-UTILS-1.1.0.20/repos/centos7 
gpgcheck = 0 
enabled = 1 
cost=1 


[ambari] 
name = this is a local repository 
baseurl = http://192.168.30.234/repos/ambari/centos7/2.x/updates/2.4.2 
gpgcheck = 0 
enabled = 1 
cost=1'
> /etc/yum.repos.d/ambari.repo" 

>>yum install ambari-server -y
>>ambari-server setup -s //提示需要jdk, 需要把上面下载的jdk包拷贝到对应的目录
>>ambari-server start
```

下面在ambari-demo路径下，换一个terminal启动host虚拟机
```
>>./up.sh 4
```
上面启动4个虚拟机，可以根据主机性能决定虚拟机个数。
在主机上增加自定义hosts:
```
>>sudo -s 'cat ./hosts >> /etc/hosts'
```
重启网络连接，打开浏览器登录
```
server.ambari.apache.org:8080
用户名:admin
密码:admin
```
