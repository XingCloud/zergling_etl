#!/bin/sh

line="-------------------"
code_home=/home/hadoop/git_project_home/zergling_etl
jar_home=${code_home}/target/LogImport-jar-with-dependencies.jar
hadoop_home=/usr/bin/
java_bin=/usr/java/jdk1.7.0_55/bin

type=yac
table_name=yac_user_action
workers=12
batch_size=1000
project_id=all
logdir=/data/log/yac/$(date -d"-5 mins" +"%Y%m%d")/
hdfs_path=/user/hadoop/history/${type}
tmp_log_path_prefix=/data/bigdata/all/${type}/tmp.log
minute=$(date +"%H%M")

echo ${logdir}
if [ $# = 1 ] ; then
  src_paths=($1)
else
  src_paths=`find ${logdir} -name "*.log"`
fi

#move file to filename.ing to avoid be processed again
for f in ${src_paths};do
  if [ -s ${f} ]; then
    `mv ${f} ${f}.ing`
  fi
done

#import per file
function import(){
  path=$1
  fullPath=${path}.ing
  if [ ! -s ${fullPath} ]; then
      echo "The ${fullPath} is empty, exit!!"
      return
  fi
  tmp_log_path="${tmp_log_path_prefix}$2"
  #clear tmp log
  echo "begin import" > ${tmp_log_path}

  daily_log_path="/data/bigdata/all/${type}/${type}_${day}.log"
  echo "begin import ${fullPath} at "$(date +"%Y-%m-%d %H:%M:%S")

  ${java_bin}/java -Xmx2048m -Xms2048m -jar ${jar_home} ${type} ${table_name} ${fullPath} ${workers} ${batch_size} ${project_id} > ${tmp_log_path}
  if grep -Fxq "Finished import log without error" ${tmp_log_path}
  then
      echo "Rename to ${fullPath} to ${path}.completed as no exception"
      mv ${fullPath} ${path}.completed
  else
      echo "Rename to ${fullPath} to ${path}.fail as get exception"
      mv ${fullPath} ${path}.fail
  fi

  echo "end import ${path} at "$(date +"%Y-%m-%d %H:%M:%S")
  rm -f ${tmp_log_path}
}

count=1
for f in ${src_paths};do
  import ${f} ${count}
  count=`expr ${count} + 1`
done

#每天凌晨0000,删除历史文件
if [ "0000" = "${minute}" ]; then
    history_day=$(date -u -d"${processing_day} 7 days ago" +%Y%m%d)
    history_path=/data/log/${type}/${history_day}
    if [ -d ${history_path} ]; then
            echo "Remove the directory ${history_path}"
            rm -rf ${history_path}
    fi
fi


echo ${line}