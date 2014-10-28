#!/bin/sh

line="-------------------"
code_home=/home/hadoop/git_project_home/zergling_etl
jar_home=${code_home}/target/LogImport-jar-with-dependencies.jar
hadoop_home=/usr/bin/
java_bin=/usr/java/jdk1.7.0_55/bin

type=ad
table_name=ad_all_log
workers=10
batch_size=500
project_id=all
hdfs_path=/user/hadoop/history/${type}
tmp_log_path=/data0/bigdata/all/${type}/tmp.log
minute=$(date +"%H%M")

if [ $# = 1 ] ; then
    fullPath=$1
else
    fullPath=/data0/log/${type}/$(date -d"-5 mins" +"%Y%m%d")/${type}_$(date -d"-5 mins" +"%Y%m%d%H%M").log
    sleep 30s #sleep 30s to wait the log been splited
fi

if [ ! -s ${fullPath} ]; then
    echo "The ${fullPath} is empty, exit!!"
    exit 1
fi

#clear tmp log
echo "begin import" > ${tmp_log_path}

daily_log_path="/data0/bigdata/all/${type}/${type}_${day}.log"
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

#每天凌晨0000,删除历史文件
if [ "0000" = "${minute}" ]; then
    history_day=$(date -u -d"${processing_day} 7 days ago" +%Y%m%d)
    history_path=/data0/log/${type}/${history_day}
    if [ -d ${history_path} ]; then
            cat ${history_path}/${type}_* > ${history_path}/${type}_${history_day}.log
            echo "Copy ${history_path}/${type}_${history_day}.log to HDFS"
            hadoop fs -copyFromLocal ${history_path}/${type}_${history_day}.log ${hdfs_path}/${history_day}.log
            echo "Remove the directory ${history_path}"
            rm -rf ${history_path}
    fi
fi

echo ${line}

#加索引
#echo "begin index ${fullPath} at "$(date +"%Y-%m-%d %H:%M:%S")
#python /data1/liqiang/es/load2es.py ${fullPath}.completed ${type}
#echo "end index ${fullPath} at "$(date +"%Y-%m-%d %H:%M:%S")