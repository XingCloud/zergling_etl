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

log_file=/data/log/runlog/nav/${input}.run
${java_bin}/java -jar ${jar_file} ${input} ${table_name} ${batch_size} ${url_restore_worker_count} ${log_store_worker_count} ${using_url_restore} ${store_to_hbase} > ${log_file} 2>&1

