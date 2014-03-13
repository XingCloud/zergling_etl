#!/bin/sh

if [ "" = "$1" ];then
  echo "You must assign a project for init.sh"
  exit 1
else
  project_id=$1
fi

if [ -d /data/bigdata/${project_id}/runlog ];then
  echo "Runlog path for ${project_id} exists."
else
  sudo mkdir -p /data/bigdata/${project_id}/runlog
  sudo chmod -R a+w /data/bigdata/${project_id}/runlog
  echo "Runlog path for ${project_id} has been created."
fi

sudo mkdir -p /data/raw/${project_id}/nav
sudo chmod -R a+w /data/raw/${project_id}/nav
echo "Raw log path /data/raw/${project_id}/nav has been created."

sudo mkdir -p /data/bigdata/${project_id}/nav
sudo chmod -R a+w /data/bigdata/${project_id}/nav
echo "Output file path /data/bigdata/${project_id}/nav has been created."

