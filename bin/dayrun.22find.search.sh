#!/bin/sh

if [ $# -eq 0 ];then
   day=`date -d "2 days ago" +%Y%m%d`
else
   day=$1
fi

daily_log_path="/data/bigdata/22find/search/search_${day}.log"
sh log_import.sh 22find search_22find search ${day} >> ${daily_log_path}
if grep -Fxq "Finished import log without error" ${daily_log_path}
then
    echo "No Exception"
else
    echo "Found Exception"
fi
