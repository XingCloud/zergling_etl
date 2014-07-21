#!/bin/sh

line="############################################"
# Code base
code_home=/home/hadoop/git_project_home/zergling_etl
jar_home=${code_home}/target/LogImport-jar-with-dependencies.jar
hadoop_home=/usr/bin/
java_bin=/usr/java/jdk1.7.0_45/bin

echo ${line}
project_id=$1
table_name=$2
#type=search or ad
type=$3

if [ "" = "$4" ];then
  echo "Using default processing day."
  processing_day=`date -u +%Y%m%d`
else
  echo "User defined processing day found."
  processing_day=$4
fi

raw_log_path=$5
current_op_file_name=$6
history_op_file_name=$7

workers=10
batch_size=500

hdfs_history_path=/user/hadoop/history/raw/${project_id}/${type}

echo ${line}
echo "[PROCESSING-DAY]="${processing_day}
echo "[PROCESSING-HISTORY-FILE]="${history_op_file_name}
echo "[PROJECT-ID]="${project_id}
echo "[DOWNLOAD-OUTPUT]="${raw_log_path}
echo "[FILE-NAME]="${current_op_file_name}
echo ${line}


#echo "Copy local to hdfs backup."

${java_bin}/java  -jar ${jar_home} ${type} ${table_name} ${raw_log_path}/${current_op_file_name} ${workers} ${batch_size} ${project_id}
echo "All done"
