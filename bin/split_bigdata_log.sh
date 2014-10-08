#!/bin/bash

access_log_path="/data0/log/all"

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

#337 game platform actions
gm_log=${access_log_path}/access.gm.log

#cookie uid map
cu_log=${access_log_path}/access.cu.log
#ad click
ac_log=${access_log_path}/access.ac.log
#nav visit
nv_log=${access_log_path}/access.nv.log

#nav add site
as_log=${access_log_path}/access.as.log

suffix=$(date -d"-5 mins" +"%Y%m%d%H%M").log

day=$(date -d"-5 mins" +"%Y%m%d")
nav_log_new_path="/data0/log/nav/${day}"
ad_log_new_path="/data0/log/ad/${day}"
nt_log_new_path="/data0/log/nt/${day}"
pc_log_new_path="/data0/log/pc/${day}"
ps_log_new_path="/data0/log/ps/${day}"
gm_log_new_path="/data0/log/gm/${day}"
cu_log_new_path="/data0/log/cu/${day}"
ac_log_new_path="/data1/user_log/ac/${day}"
nv_log_new_path="/data1/user_log/nv/${day}"
as_log_new_path="/data1/user_log/as/${day}"

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

if [ ! -d ${gm_log_new_path} ]; then
  mkdir -p ${gm_log_new_path}
  chown hadoop:hadoop ${gm_log_new_path}
fi

if [ ! -d ${cu_log_new_path} ]; then
  mkdir -p ${cu_log_new_path}
  chown hadoop:hadoop ${cu_log_new_path}
fi

if [ ! -d ${ac_log_new_path} ]; then
  mkdir -p ${ac_log_new_path}
  chown hadoop:hadoop ${ac_log_new_path}
fi

if [ ! -d ${nv_log_new_path} ]; then
  mkdir -p ${nv_log_new_path}
  chown hadoop:hadoop ${nv_log_new_path}
fi

if [ ! -d ${as_log_new_path} ]; then
  mkdir -p ${as_log_new_path}
  chown hadoop:hadoop ${as_log_new_path}
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

mv ${gm_log} ${gm_log_new_path}/gm_${suffix}
chown hadoop:hadoop ${gm_log_new_path}/gm_${suffix}

mv ${cu_log} ${cu_log_new_path}/cu_${suffix}
chown hadoop:hadoop ${cu_log_new_path}/cu_${suffix}

mv ${ac_log} ${ac_log_new_path}/ac_${suffix}
chown hadoop:hadoop ${ac_log_new_path}/ac_${suffix}

mv ${nv_log} ${nv_log_new_path}/nv_${suffix}
chown hadoop:hadoop ${nv_log_new_path}/nv_${suffix}

mv ${as_log} ${as_log_new_path}/as_${suffix}
chown hadoop:hadoop ${as_log_new_path}/as_${suffix}

/usr/sbin/nginx -s reopen

echo "end split ${suffix}"
