#!/bin/sh

line="############################################"
# Code base
code_home=/home/hadoop/git_project_home/zergling_etl

# Artifact Id
aid=zergling_etl

# Version
env="production"

if [ "" = "$1" ];then
  branch=master
else
  echo "User defined branch found($1)"
  branch=$2
fi

echo "[CHECK-POINT] - Building package."
echo ${line}
echo "[CODE-HOME] - "${code_home}
echo "[CURRENT-BRANCH] - "${branch}
echo "[ENV] - "${env}

echo ${line}
echo "[CHECK-POINT] - Update code from VCS"
cd ${code_home}
git pull
git checkout ${branch}

if [ $? -ne 0 ];then
  echo "Git update/checkout failed."
  exit 1
else
  echo "Git Update/checkout successfully."
fi

echo "[CHECK-POINT] - Packaging."
#mvn -f ${code_home}/pom.xml clean package -DskipTests=true -Daid=${aid} -Denv=${env}
#mvn -f ${code_home}/pom.xml clean compile assembly:single -DskipTests=true -Daid=${aid} -Denv=${env}
mvn -f ${code_home}/pom.xml clean package -DskipTests
echo "[CHECK-POINT] - Packaging is done."
