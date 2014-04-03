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

#每天凌晨0000,删除历史文件(暂不放到HDFS)
minute=$(date +"%H%M")
if [ "0000" = "${minute}" ]; then
    history_path=/data/log/ad/$(date -u -d"${processing_day} 15 days ago" +%Y%m%d)
    if [ -d ${history_path} ]; then
            echo "Remove the directory ${history_path}"
            rm -rf ${history_path}
    fi
fi


if [ $# = 1 ] ; then
    fullPath=$1
else
    fullPath=/data/log/ad/$(date +"%Y%m%d")/ad_$(date +"%Y%m%d%H%M").log
    sleep 30s #sleep 30s to wait the log been splited
fi

daily_log_path="/data/bigdata/all/ad/ad_${day}.log"
tmp_log_path=/data/bigdata/all/ad/tmp.log
#echo "minute:${minute}"
echo "begin import ${fullPath} at "$(date +"%Y-%m-%d %H:%M:%S")

${java_bin}/java -jar ${jar_home} ${type} ${table_name} ${fullPath} ${workers} ${batch_size} ${project_id} > ${tmp_log_path}
if grep -Fxq "Finished import log without error" ${tmp_log_path}
then
    echo "Rename to ${fullPath} to ${fullPath}.completed as no exception"
    mv ${fullPath} ${fullPath}.completed
else
    echo "Rename to ${fullPath} to ${fullPath}.fail as get exception"
    mv ${fullPath} ${fullPath}.fail
fi

echo "end import ${fullPath} at "$(date +"%Y-%m-%d %H:%M:%S")
echo ${line}