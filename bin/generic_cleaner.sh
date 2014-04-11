#!/bin/sh

hadoop_home=/usr/bin/

source_file_home=/data/log
local_history_home=/data/local_history
hdfs_history_home=/user/hadoop/history

if [ "" = "$1" ];then
  processing_date=`date -d"-1 days" +%Y%m%d`
else
  processing_date=$1
fi

type=nav
input_path=${source_file_home}/${type}/${processing_date}

if [ ! -d ${input_path} ];then
  echo "Input date(${processing_date}) does not exist."
  exit 1
fi

merged_and_local_history_file=${local_history_home}/${type}/${type}.${processing_date}.log
echo "Merging(Type=${type}, date=${processing_date})"
cat ${input_path}/* >> ${merged_and_local_history_file}

compressed_local_history_file=${merged_and_local_history_file}.tar.gz
echo "Compressing(Type=${type}, date=${processing_date})"
tar zcvf ${compressed_local_history_file} ${merged_and_local_history_file}

echo "Backuping(Type=${type}, date=${processing_date})"
history_file=${hdfs_history_home}/${type}/${type}.${processing_date}.log.tar.gz
${hadoop_home}/hadoop fs -test -e ${history_file}
if [ $? -ne 0 ];then
  echo "Copy directly(${history_file})."
else
  echo "Remove exists history file.(${history_file})."
  ${hadoop_home}/hadoop fs -rm -r ${history_file}
fi
${hadoop_home}/hadoop fs -copyFromLocal ${compressed_local_history_file} ${history_file}

history_processing_date=`date -d"${processing_date} -7 days" +%Y%m%d`
too_old_history_path1=${source_file_home}/${type}/${history_processing_date}
too_old_history_path2=${local_history_home}/${type}/${type}.${history_processing_date}.log
too_old_history_path3=${local_history_home}/${type}/${type}.${history_processing_date}.log.tar.gz
echo "Cleaning(${too_old_history_path1})"
rm -rf ${too_old_history_path1}
echo "Cleaning(${too_old_history_path2})"
rm -rf ${too_old_history_path2}
echo "Cleaning(${too_old_history_path3})"
rm -rf ${too_old_history_path3}
echo "All done"
