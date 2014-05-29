#!/bin/sh

line="############################################"
# Code base
code_home=/home/hadoop/git_project_home/zergling_etl
jar_home=${code_home}/target/zergling_etl-jar-with-dependencies.jar
hadoop_home=/usr/bin/
java_bin=/usr/java/jdk1.7.0_55/bin

echo ${line}
if [ "" = "$1" ];then
  echo "Using default processing day."
  processing_day=`date -u +%Y-%m-%d`
else
  echo "User defined processing day found."
  processing_day=$1
fi
only_show=$2
project_id=$3
table_name_suffix=$4
workers=20
batch_size=100
time_zone=GMT-06:00

processing_history_day=`date -u -d"${processing_day} 7 days ago" +%Y-%m-%d`
raw_log_web=http://log.goo.mx/clicklogs/log.goo.mx.access-${processing_day}.tar.gz
raw_log_path=/data/raw/${project_id}/nav
default_unpack_name=home/elex/serversoft/nginx/log/log.goo.mx.access.log
hdfs_history_path=/user/hadoop/history/raw/${project_id}/nav
current_op_file_name=${project_id}.${processing_day}
history_op_file_name=${project_id}.${processing_history_day}
output_path=/data/bigdata/${project_id}/nav

echo ${line}
echo "[PROCESSING-DAY]="${processing_day}
echo "[PROCESSING-HISTORY-DAY]="${processing_history_day}
echo "[PROJECT-ID]="${project_id}
echo "[HTTP-RAW-LOG]="${raw_log_web}
echo "[DOWNLOAD-OUTPUT]="${raw_log_path}
echo ${line}

wget ${raw_log_web} -O ${raw_log_path}/${current_op_file_name}.tar.gz
if [ $? -ne 0 ];then
  echo "Downloading from http://log.goo.mx/clicklogs is failed."
  exit 1
else
  echo "Downloading from http://log.goo.mx/clicklogs is ok."
fi
echo ${line}
echo "Unpacking and flattening path."
tar zxvf ${raw_log_path}/${current_op_file_name}.tar.gz -C ${raw_log_path}
mv  ${raw_log_path}/home/elex/serversoft/nginx/log/log.goo.mx.access.log ${raw_log_path}/${current_op_file_name}.log
rm -rf ${raw_log_path}/home
echo ${line}
echo "Copy local to hdfs backup."

${hadoop_home}/hadoop fs -test -d ${hdfs_history_path}
if [ $? -ne 0 ];then
  echo "Backup path does not exist. Create it now(${hdfs_history_path})."
  ${hadoop_home}/hadoop fs -mkdir ${hdfs_history_path}
fi

${hadoop_home}/hadoop fs -test -e ${hdfs_history_path}/${current_op_file_name}.tar.gz
if [ $? -ne 0 ];then
  echo "Copy directly(${hdfs_history_path}/${current_op_file_name}.tar.gz)."
else
  echo "Remove exists history file.(${hdfs_history_path}/${current_op_file_name}.tar.gz)."
  ${hadoop_home}/hadoop fs -rm -r  ${hdfs_history_path}/${current_op_file_name}.tar.gz
fi
${hadoop_home}/hadoop fs -copyFromLocal ${raw_log_path}/${current_op_file_name}.tar.gz ${hdfs_history_path}
echo ${line}

${hadoop_home}/hadoop fs -test -e ${hdfs_history_path}/${history_op_file_name}.tar.gz
if [ $? -ne 0 ];then
  echo "Old raw file does not exist on hdfs(${hdfs_history_path}/${history_op_file_name}.tar.gz), skip to remove local old file."
else
  echo "Remove old file from local(${hdfs_history_path}/${history_op_file_name}.tar.gz)."
  rm -rf ${raw_log_path}/${history_op_file_name}.log
  rm -rf ${raw_log_path}/${history_op_file_name}.tar.gz
fi

#mvn -f ${code_home}/pom.xml exec:java -Dexec.mainClass="com.elex.bigdata.zergling.etl.NavigatorETL" -Dexec.args="${project_id} ${raw_log_path}/${current_op_file_name}.log ${output_path}/${project_id}${table_name_suffix}.${processing_day}.nav.log nav_${project_id}${table_name_suffix} ${only_show} ${workers} ${batch_size}" -Dexec.classpathScope=runtime
${java_bin}/java -jar ${jar_home} ${project_id} ${raw_log_path}/${current_op_file_name}.log ${output_path}/${project_id}${table_name_suffix}.${processing_day}.nav.log nav_${project_id}${table_name_suffix} ${only_show} ${workers} ${batch_size} ${time_zone}

echo "All done"