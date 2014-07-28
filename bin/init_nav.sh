#!/bin/sh

if [ "" = "$1" ];then
  echo "You must assign a project for init.sh"
  exit 1
else
  project_id=$1
fi

if [ -d /data0/bigdata/${project_id}/runlog ];then
  echo "Runlog path for ${project_id} exists."
else
  sudo mkdir -p /data0/bigdata/${project_id}/runlog
  sudo chmod -R a+w /data0/bigdata/${project_id}/runlog
  echo "Runlog path for ${project_id} has been created."
fi

sudo mkdir -p /data0/raw/${project_id}/nav
sudo chmod -R a+w /data0/raw/${project_id}/nav
echo "Raw log path /data0/raw/${project_id}/nav has been created."

sudo mkdir -p /data0/bigdata/${project_id}/nav
sudo chmod -R a+w /data0/bigdata/${project_id}/nav
echo "Output file path /data0/bigdata/${project_id}/nav has been created."

