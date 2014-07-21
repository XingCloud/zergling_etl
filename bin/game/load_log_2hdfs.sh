#!/bin/sh

if [ $# -eq 0 ];then
   offset=1
else
   offset=$1
fi

if [ ${offset} -eq 1 ];then
    today=`date -u +%Y%m%d`
    todayfmt=`date -u +%Y-%m-%d`
else
    today=`date -d "${offset-1} days ago" +%Y%m%d`
    todayfmt=`date -d "${offset-1} days ago" +%Y-%m-%d`
fi

yesterday=`date -d "${offset} days ago" +%Y%m%d`
yesterdayfmt=`date -d "${offset} days ago" +%Y-%m-%d`
tdbyesterday=`date -d "${offset+1} days ago" +%Y%m%d`

gmpath="/data/log/gm"
destpath="/data/log/gm/play"
destfile="${destpath}/${yesterday}.log"
fmtdestfile="${destpath}/${yesterday}_fmt.log"
tdbyesterdaylastfile="${gmpath}/${tdbyesterday}/gm_${tdbyesterday}2355.log.completed"
hdfspath="/user/gm/"

echo "Collect ${yesterday} play log"
`cat ${gmpath}/${yesterday}/* | grep play | grep -v ${todayfmt} > ${destfile}`
if [ -s ${tdbyesterdaylastfile} ]; then
    `grep ${yesterdayfmt} ${tdbyesterdaylastfile} | grep play >> ${destfile}`
fi

echo "Parse ${yesterday} play log"
python /home/hadoop/git_project_home/zergling_etl/bin/game/format_source_log.py ${destfile} ${fmtdestfile} || { echo "Format failed"; exit 1; }
`rm ${destfile}`

echo "Load ${fmtdestfile} to hdfs "
`hadoop fs -copyFromLocal ${fmtdestfile} ${hdfspath}` || { echo "Load failed"; exit 1; }

