#!/bin/sh

line="############################################"
# Code base
code_home=/home/hadoop/git_project_home/zergling_etl
jar_home=${code_home}/target/zergling_etl-jar-with-dependencies.jar
hadoop_home=/usr/bin/
java_bin=/usr/java/jdk1.7.0_45/bin

echo ${line}
if [ "" = "$1" ];then
  echo "Using default processing day."
  processing_day=`date -u +%Y%m%d`
else
  echo "User defined processing day found."
  processing_day=$1
fi
project_id=$2
table_name=$3
workers=10
batch_size=100
time_zone=GMT-06:00

processing_history_day=`date -u -d"${processing_day} 7 days ago" +%Y%m%d`
raw_log_web=http://22find-log.22find.com/22find.log.${processing_day}
raw_log_path=/data/raw/${project_id}/search
hdfs_history_path=/user/hadoop/history/raw/${project_id}/search
current_op_file_name=${project_id}.${processing_day}.log
history_op_file_name=${project_id}.${processing_history_day}.log
#output_path=/data/bigdata/${project_id}/nav

echo ${line}
echo "[PROCESSING-DAY]="${processing_day}
echo "[PROCESSING-HISTORY-DAY]="${processing_history_day}
echo "[PROJECT-ID]="${project_id}
echo "[HTTP-RAW-LOG]="${raw_log_web}
echo "[DOWNLOAD-OUTPUT]="${raw_log_path}
echo "[FILE-NAME]="${current_op_file_name}
echo ${line}

wget ${raw_log_web} -O ${raw_log_path}/${current_op_file_name}
if [ $? -ne 0 ];then
  echo "Downloading from ${raw_log_web} is failed."
  exit 1
else
  echo "Downloading from ${raw_log_web} is ok."
fi
echo ${line}

echo "Copy local to hdfs backup."

${hadoop_home}/hadoop fs -test -d ${hdfs_history_path}
if [ $? -ne 0 ];then
  echo "Backup path does not exist. Create it now(${hdfs_history_path})."
  ${hadoop_home}/hadoop fs -mkdir ${hdfs_history_path}
fi

${hadoop_home}/hadoop fs -test -e ${hdfs_history_path}/${current_op_file_name}
if [ $? -ne 0 ];then
  echo "Copy directly(${hdfs_history_path}/${current_op_file_name})."
else
  echo "Remove exists history file.(${hdfs_history_path}/${current_op_file_name})."
  ${hadoop_home}/hadoop fs -rm -r  ${hdfs_history_path}/${current_op_file_name}
fi
${hadoop_home}/hadoop fs -copyFromLocal ${raw_log_path}/${current_op_file_name} ${hdfs_history_path}
echo ${line}

${hadoop_home}/hadoop fs -test -e ${hdfs_history_path}/${history_op_file_name}
if [ $? -ne 0 ];then
  echo "Old raw file does not exist on hdfs(${hdfs_history_path}/${history_op_file_name}), skip to remove local old file."
else
  echo "Remove old file from local(${hdfs_history_path}/${history_op_file_name})."
  rm -rf ${raw_log_path}/${history_op_file_name}
fi

#mvn -f ${code_home}/pom.xml exec:java -Dexec.mainClass="com.elex.bigdata.zergling.etl.NavigatorETL" -Dexec.args="${project_id} ${raw_log_path}/${current_op_file_name}.log ${output_path}/${project_id}${table_name_suffix}.${processing_day}.nav.log nav_${project_id}${table_name_suffix} ${only_show} ${workers} ${batch_size}" -Dexec.classpathScope=runtime
${java_bin}/java -jar ${jar_home} search ${table_name} ${history_op_file_name} ${workers} ${batch_size}
echo "All done"
