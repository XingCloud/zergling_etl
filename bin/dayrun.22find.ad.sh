#!/bin/sh

line="-------------------"
code_home=/home/hadoop/git_project_home/zergling_etl
jar_home=${code_home}/target/LogImport-jar-with-dependencies.jar
hadoop_home=/usr/bin/
java_bin=/usr/java/jdk1.7.0_45/bin

type=ad
table_name=ad_all_log
workers=10
batch_size=500
project_id=all

#每天凌晨0000的时候，合并HDFS的文件，删除历史文件
minute=$(date +"%H%M")

if [ $# = 1 ] ; then
    fullPath=$1
else
    fullPath=/data/log/ad/$(date +"%Y%m%d")/ad_$(date +"%Y%m%d%H%M").log
fi

sleep 30s #sleep 30s to wait the log been splited

daily_log_path="/data/bigdata/all/ad/ad_${day}.log"
echo line
echo "minute:${minute}"
echo "Import "${fullPath}

${java_bin}/java -jar ${jar_home} ${type} ${table_name} ${fullPath} ${workers} ${batch_size} ${project_id}
#if grep -Fxq "Finished import log without error" ${daily_log_path}
#then
#    echo "No Exception"
#else
#    echo "Found Exception"
#fi
echo ${line}