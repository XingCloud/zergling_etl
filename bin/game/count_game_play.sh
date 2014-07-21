#!/bin/sh

if [ $# -eq 0 ];then
   yesterday=`date -d "1 days ago" +%Y%m%d`
else
   yesterday=$1
fi


hdfspath="/user/gm/${yesterday}_fmt.log"

ssh dmnode5 sh /home/hadoop/git_project_home/zergling_etl/game/bin/load_log_2hdfs.sh || { echo "command failed"; exit 1; }

echo "Load ${hdfspath} into table game_play_action"
hive -e "load data inpath '${hdfspath}' overwrite into table game_play_action partition(day='${yesterday}')"

echo "Load game_play_action/${yesterday} to game_play_count"
hive -e "insert into table game_play_count select day,uid,gid,count(*) from game_play_action where day=${yesterday} group by day,uid,gid"

echo "Count most play"
hive -e "select gid,sum(ct) as total from game_play_count group by gid order by total desc limit 100" > /data/game/${yesterday}_mt.log || { echo "command failed"; exit 1; }

echo "Count most user"
hive -e "select gid,count(distinct uid) as total from game_play_count group by gid order by total desc limit 100" > /data/game/${yesterday}_mp.log || { echo "command failed"; exit 1; }

python /home/hadoop/scripts/game/format_game_result.py ${yesterday}

countmd5=`/usr/java/jdk/bin/java -jar /home/hadoop/MD5Digest.jar /data/game/${yesterday}_fmt.log`
cp /data/game/${yesterday}_fmt.log /data/game/${countmd5}

scp -r /data/game/${countmd5} elex@162.243.114.236:~/rec/count
echo "end"