#!/bin/sh

echo "Daily 22find nav log etl."

processing_day=`date -u +%Y-%m-%d`
project_id=22find

sh /home/hadoop/git_project_home/zergling_etl/bin/run_zergling_etl.sh ${processing_day} false ${project_id}
