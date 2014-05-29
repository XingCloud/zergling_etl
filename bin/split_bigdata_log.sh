#!/bin/bash

access_log_path="/data/log/all"

# navigator log
nav_log=${access_log_path}/access.nav.log

# ad click log
ad_log=${access_log_path}/access.ad.log

# newtab record users' first screen content
nt_log=${access_log_path}/access.nt.log

# newtab(ep) record users' url log
pc_log=${access_log_path}/access.pc.log

# newtab(ep) record users' stay-time in some page log
ps_log=${access_log_path}/access.ps.log

suffix=$(date -d"-5 mins" +"%Y%m%d%H%M").log

day=$(date -d"-5 mins" +"%Y%m%d")
nav_log_new_path="/data/log/nav/${day}"
ad_log_new_path="/data/log/ad/${day}"
nt_log_new_path="/data/log/nt/${day}"
pc_log_new_path="/data/log/pc/${day}"
ps_log_new_path="/data/log/ps/${day}"

if [ ! -d ${nav_log_new_path} ]; then
  mkdir -p ${nav_log_new_path}
  chown hadoop:hadoop ${nav_log_new_path}
fi

if [ ! -d ${ad_log_new_path} ]; then
  mkdir -p ${ad_log_new_path}
  chown hadoop:hadoop ${ad_log_new_path}
fi

if [ ! -d ${nt_log_new_path} ]; then
  mkdir -p ${nt_log_new_path}
  chown hadoop:hadoop ${nt_log_new_path}
fi

if [ ! -d ${pc_log_new_path} ]; then
  mkdir -p ${pc_log_new_path}
  chown hadoop:hadoop ${pc_log_new_path}
fi

if [ ! -d ${ps_log_new_path} ]; then
  mkdir -p ${ps_log_new_path}
  chown hadoop:hadoop ${ps_log_new_path}
fi

echo "begin split ${suffix} "

mv ${nav_log} ${nav_log_new_path}/nav_${suffix}
chown hadoop:hadoop ${nav_log_new_path}/nav_${suffix}

mv ${ad_log} ${ad_log_new_path}/ad_${suffix}
chown hadoop:hadoop ${ad_log_new_path}/ad_${suffix}

mv ${nt_log} ${nt_log_new_path}/nt_${suffix}
chown hadoop:hadoop ${nt_log_new_path}/nt_${suffix}

mv ${pc_log} ${pc_log_new_path}/pc_${suffix}
chown hadoop:hadoop ${pc_log_new_path}/pc_${suffix}

mv ${ps_log} ${ps_log_new_path}/ps_${suffix}
chown hadoop:hadoop ${ps_log_new_path}/ps_${suffix}

/usr/sbin/nginx -s reopen

echo "end split ${suffix}"
