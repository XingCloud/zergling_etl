#!/bin/sh


processing_date=`date -d"-5 mins" +%Y%m%d`
processing_min5=`date -d"-5 mins" +%H%M`
scripts_home=/home/hadoop/git_project_home/zergling_etl/bin

# Run nav.all
sh ${scripts_home}/min5run.all.nav.sh /data/log/nav/${processing_date}/nav_${processing_date}${processing_min5}.log ${processing_date} ${processing_min5} &
