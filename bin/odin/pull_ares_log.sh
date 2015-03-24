#! /bin/sh

bin=`dirname $0`
bin=`cd "$bin">/dev/null; pwd`

if [ $# = 1 ] ; then
    day=$1
else
    day=`date -d "1 days ago" +%Y%m%d`
fi

host=ares
remote_log_file=/home/elex/apps/ares.adserver/logs/access.log.${day}
local_log_file=/data1/user_log/ares/${day}/access.${day}.log

mkdir -p /data1/user_log/ares/${day}/

echo "begin scp ares $day"
scp elex@${host}:${remote_log_file} ${local_log_file}

echo "format ares log"
python $bin/format_log.py ares all ${day}
echo "done"
