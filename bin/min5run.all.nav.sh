#!/bin/sh

line="############################################"
# Code base
code_home=/home/hadoop/git_project_home/zergling_etl
jar_file=${code_home}/target/zergling_etl.AllInOneNavigatorETL.jar-with-dependencies.jar
java_bin=/usr/java/jdk1.7.0_45/bin

if [ "" = "$1" ];then
  echo "Input file is necessary."
  exit 1
else
  input=${1}
fi

table_name=b
batch_size=50
url_restore_worker_count=10
log_store_worker_count=5
using_url_restore=true
store_to_hbase=true

current_date=`date +%Y%m%d`
current_min5=`date +%H%M`
log_file_path=/data/log/runlog/nav/${current_date}
if [ ! -d ${log_file_path} ];then
  sudo mkdir -p ${log_file_path}
  sudo chmod a+w ${log_file_path}
fi

log_file=${log_file_path}/run.%{current_min5}.log

${java_bin}/java -jar ${jar_file} ${input} ${table_name} ${batch_size} ${url_restore_worker_count} ${log_store_worker_count} ${using_url_restore} ${store_to_hbase} > ${log_file} 2>&1

