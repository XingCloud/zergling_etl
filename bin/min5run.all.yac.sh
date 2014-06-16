#!/bin/sh

line="-------------------"
code_home=/home/hadoop/git_project_home/zergling_etl
jar_home=${code_home}/target/LogImport-jar-with-dependencies.jar
hadoop_home=/usr/bin/
java_bin=/usr/java/jdk1.7.0_55/bin

type=yac
table_name=yac_user_action
workers=10
batch_size=500
project_id=all
logdir=/data/log/$(date -d"-5 mins" +"%Y%m%d")/
hdfs_path=/user/hadoop/history/${type}
tmp_log_path=/data/bigdata/all/${type}/tmp.log

if [ $# = 1 ] ; then
  src_paths=($1)
else
  src_paths=`find ${logdir} -name *.dat`
fi

#move file to filename.ing to avoid be processed again
for f in src_paths;do
  if [ -s ${f} ]; then
    `mv ${f} ${f}.ing`
  fi
done

echo ${line}

for f in src_paths;do
  import ${f}
done

#import per file
function import(){
  path=$1
  fullPath=${path}.ing
  if [ ! -s ${fullPath} ]; then
      echo "The ${fullPath} is empty, exit!!"
      return
  fi

  #clear tmp log
  echo "begin import" > ${tmp_log_path}

  daily_log_path="/data/bigdata/all/${type}/${type}_${day}.log"
  echo "begin import ${fullPath} at "$(date +"%Y-%m-%d %H:%M:%S")

  #${java_bin}/java -Xmx1024m -Xms512m -jar ${jar_home} ${type} ${table_name} ${fullPath} ${workers} ${batch_size} ${project_id} > ${tmp_log_path}
  if grep -Fxq "Finished import log without error" ${tmp_log_path}
  then
      echo "Rename to ${fullPath} to ${path}.completed as no exception"
      mv ${fullPath} ${path}.completed
  else
      echo "Rename to ${fullPath} to ${path}.fail as get exception"
      mv ${fullPath} ${path}.fail
  fi

  echo "end import ${path} at "$(date +"%Y-%m-%d %H:%M:%S")

}
