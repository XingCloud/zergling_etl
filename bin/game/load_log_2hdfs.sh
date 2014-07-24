#!/bin/sh

if [ $# -eq 0 ];then
   offset=1
else
   offset=$1
fi

todayOff=`expr $offset - 1`
tbyoff=`expr $offset + 1`

todayfmt=`date -d "${todayOff} days ago" +%Y-%m-%d`


yesterday=`date -d "${offset} days ago" +%Y%m%d`
yesterdayfmt=`date -d "${offset} days ago" +%Y-%m-%d`
tdbyesterday=`date -d "${tbyoff} days ago" +%Y%m%d`

gmpath="/data/log/gm"
destpath="/data/log/gm/play"
destfile="${destpath}/${yesterday}.log"
fmtdestfile="${destpath}/${yesterday}_fmt.log"
tdbyesterdaylastfile="${gmpath}/${tdbyesterday}/gm_${tdbyesterday}2355.log.completed"
hdfspath="/user/gm/"

echo "Collect ${yesterday} play log to ${destfile}"
echo "cat ${gmpath}/${yesterday}/* | grep play | grep -v ${todayfmt} > ${destfile}"
`cat ${gmpath}/${yesterday}/* | grep play | grep -v ${todayfmt} > ${destfile}`
if [ -s ${tdbyesterdaylastfile} ]; then
    `grep ${yesterdayfmt} ${tdbyesterdaylastfile} | grep play >> ${destfile}`
fi

echo "Parse ${yesterday} play log"
python /home/hadoop/git_project_home/zergling_etl/bin/game/format_source_log.py ${destfile} ${fmtdestfile} || { echo "Format failed"; exit 1; }
`rm ${destfile}`

hadoop fs -test -e ${hdfspath}/${yesterday}_fmt.log
if [ $? -ne 0 ];then
   echo "Copy directly(${hdfspath})."
else
   echo "Remove exists history file.(${hdfspath})."
   hadoop fs -rm -r ${hdfspath}/${yesterday}_fmt.log
fi

echo "Load ${fmtdestfile} to hdfs "
`hadoop fs -copyFromLocal ${fmtdestfile} ${hdfspath}` || { echo "Load failed"; exit 1; }

