#!/bin/sh
export JAVA_HOME=/opt/jdk
export JRE_HOME=/opt/jdk
export PATH=.:/opt/jdk/bin:$PATH
cd /opt/que01/back/HE-6025.Receiver_BUS
ps -ef|grep java |grep receiver-0.2.2
# 4-27-SNAPSHOT.jar
# kill `ps -ef|grep java |grep receiver-0.2.20.04-27-SNAPSHOT.jar|  awk '{print $2}'`
kill `ps -ef|grep java |grep receiver-0.2.21|grep jar|  awk '{print $2}'`
sleep 1
ps -ef|grep java |grep receiver-0.2.2
# 4-27-SNAPSHOT.jar
# kill -n 9 `ps -ef|grep java |grep receiver-0.2.20.04-27-SNAPSHOT.jar|  awk '{print $2}'`
cp HE-6025.Receiver.log HE-6025.Receiver.log.old
cp HE-6025.Receiver_BUS.err HE-6025.Receiver_BUS.err.old
rm -f HE-6025.Receiver.log
rm -f HE-6025.Receiver_BUS.err
echo $PATH
java -server -Djava.net.preferIPv4Stack=true -Xms512m -Xmx4g -jar receiver-0.2.21.08-30-SNAPSHOT.jar 2>/opt/que01/back/HE-6025.Receiver_BUS/HE-6025.Receiver_BUS.err
#receiver-0.2.21.07-07-SNAPSHOT.jar
#receiver-0.2.21.04-09-SNAPSHOT.jar
#receiver-0.2.21.03-30-SNAPSHOT.jar 
