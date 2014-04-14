#!/bin/sh

if [ "" = "$1" ];then
  processing_date=`date -d"-1 days" +%Y%m%d`
else
  processing_date=$1
fi

if [ "" = "$2" ];then
  nation=br
else
  nation=$2
fi

for type in ad nav
do
  echo "[TYPE] - ${type}"
  for projectId in 22find 337 awesomehp delta-homes nationzoom portaldosites qvo6 sweet-page v9
  do
    num=`grep p=${projectId} /data/log/${type}/${processing_date}/* | grep nation=${nation} | wc -l`
    echo "  ${projectId}=${num}"
  done
done

