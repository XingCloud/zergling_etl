#!/bin/sh

line="############################################"
# Code base
jar_home=/home/hadoop/git_project_home/zergling_etl/target/zergling_etl.jar
hadoop_home=/usr/bin/

echo ${line}
if [ "" = "$1" ];then
  echo "Using default processing day."
  processing_day=`date -u +%Y-%m-%d`
else
  echo "User defined processing day found."
  processing_day=$1
fi
raw_log_web=http://log.goo.mx/clicklogs/log.goo.mx.access-${processing_day}.tar.gz
raw_log_path=/data/raw/22find/nav/
default_unpack_name=home/elex/serversoft/nginx/log/log.goo.mx.access.log
project_id=22find
hdfs_history_path=/user/hadoop/history/raw/22find/nav
echo ${line}
echo "[PROCESSING-DAY]="${processing_day}
echo "[PROJECT-ID]="${project_id}
echo "[HTTP-RAW-LOG]="${raw_log_web}
echo "[DOWNLOAD-OUTPUT]="${raw_log_path}
echo ${line}

wget ${raw_log_web} -O /data/raw/22find/nav/${project_id}.${processing_day}.tar.gz
if [ $? -ne 0 ];then
  echo "Downloading from http://log.goo.mx/clicklogs is failed."
  exit 1
else
  echo "Downloading from http://log.goo.mx/clicklogs is ok."
fi
echo ${line}
echo "Unpacking and flattening path."
tar zxvf ${raw_log_path}/22find.2014-02-24.tar.gz -C ${raw_log_path}
mv  ${raw_log_path}/home/elex/serversoft/nginx/log/log.goo.mx.access.log ${raw_log_path}/22find.${processing_day}.log
rm -rf ${raw_log_path}/home
echo ${line}
echo "Copy local to hdfs backup."
${hadoop_home}/hadoop fs -test -d ${hdfs_history_path}
if [ $? -ne 0 ];then
  echo "Backup path does not exist. Create it now(${hdfs_history_path})."
  ${hadoop_home}/hadoop fs -mkdir ${hdfs_history_path}
fi
echo ${line}
#mvn -f D:/git_home_new/zergling_etl/pom.xml exec:java -Dexec.mainClass="com.elex.bigdata.zergling.etl.NavigatorETL" -Dexec.args="2014-02-24 22find D:/22find/log.goo.mx.access.log D:/22find/22find.nav.log" -Dexec.classpathScope=runtime