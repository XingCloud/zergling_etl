#!/bin/sh

code_home=/home/hadoop/git_project_home/zergling_etl
jar_home=${code_home}/target/zergling_etl.CurrentMin5.jar-with-dependencies.jar
java_bin=/usr/java/jdk1.7.0_45/bin

processing_history_day=`${java_bin}/java -jar ${jar_home}`
