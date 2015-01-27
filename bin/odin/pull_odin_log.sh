#! /bin/sh

bin=`dirname $0`
bin=`cd "$bin">/dev/null; pwd`

if [ $# = 1 ] ; then
    day=$1
else
    day=`date -d "1 days ago" +%Y%m%d`
fi

remote_log_dir=/home/elex/apps/log/odin.goo.mx

echo "begin $day"
for host in odin0 odin1
do
    local_log_dir=/data1/user_log/${host}/${day}

    if [ ! -d ${local_log_dir} ]; then
      mkdir -p ${local_log_dir}
      chown hadoop:hadoop ${local_log_dir}
    fi

    filename=${host}_${day}.tar.gz
    echo "tar ${filename}"
    ssh elex@${host} "cd ${remote_log_dir} && tar -czf ${filename} odin-*${day}*"

    echo "scp ${filename}"
    scp elex@${host}:${remote_log_dir}/${filename} ${local_log_dir}

    tar -xzf ${local_log_dir}/${filename} -C ${local_log_dir}

    echo "clean ${filename}"
    ssh elex@${host} rm ${remote_log_dir}/${filename}
    rm ${local_log_dir}/${filename}
done

echo "format odin log"
python $bin/format_log.py ad_imp all ${day}
echo "done"
