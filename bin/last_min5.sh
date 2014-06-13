#!/bin/sh

code_home=/home/hadoop/git_project_home/zergling_etl
jar_home=${code_home}/target/zergling_etl.CurrentMin5.jar-with-dependencies.jar
java_bin=/usr/java/jdk1.7.0_55/bin

current_min5=`${java_bin}/java -jar ${jar_home}`
echo ${current_min5}