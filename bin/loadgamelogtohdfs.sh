#!/bin/sh

today=`date -u +%Y%m%d`
todayfmt=`date -u +%Y-%m-%d`
yesterday=`date -d "1 days ago" +%Y%m%d`
yesterdayfmt=`date -d "1 days ago" +%Y-%m-%d`
tdbyesterday=`date -d "2 days ago" +%Y%m%d`

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
`python /home/hadoop/git_project_home/zergling_etl/bin/gamelogformat.py ${destfile} ${fmtdestfile}` || { echo "command failed"; exit 1; }
`rm ${destfile}`

echo "Copy ${fmtdestfile} to hdfs "
`hadoop fs -copyFromLocal ${fmtdestfile} ${hdfspath}` || { echo "command failed"; exit 1; }

