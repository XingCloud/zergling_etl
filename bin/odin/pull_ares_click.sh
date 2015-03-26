#! /bin/sh

bin=`dirname $0`
bin=`cd "$bin">/dev/null; pwd`

if [ $# = 1 ] ; then
    day=$1
else
    day=`date -d "1 days ago" +%Y%m%d`
fi

remotedf=`date -d "${day}" +%Y-%m-%d`
filename=click-${remotedf}.log.gz

path=/data1/user_log/ares_click/${day}/
mkdir -p ${path}
cd ${path}
/usr/bin/lftp log:MhxzKhl1234.@click.v9.com  -e 'cd /home/elex/apps/logs/ares.jumper/; get ${filename} ; quit;'

gunzip ${filename}
rm ${filename}

echo "format ares click log"
python $bin/format_log.py ares_click all ${day}
echo "done"
