#daily merge the ad_impression|search|nav_visit log to req_detail

if [ $# = 1 ] ; then
    day=$1
else
    day=`date +%Y%m%d`
fi

hive -e " insert into table odin.req_detail partition(day='$day')
          select nv.pid, nv.time, nv.reqid, nv.uid, nv.ip, nv.nation, nv.os, nv.width, nv.height, ai.im, s.kw
          from odin.nav_visit nv
          left outer join
          (select reqid, concat_ws(',',collect_set(concat(time,'|',slot,'|',adid)) ) im from odin.ad_impression  where day = '$day' group by reqid) ai on nv.reqid = ai.reqid
          left outer join
          (select reqid,concat_ws(',',collect_set(concat(time, '=>', regexp_replace(keyword,'=>|,',' ')))) kw  from odin.search where day = '$day' group by reqid ) s on nv.reqid=s.reqid
          where nv.day =  '$day'
          order by nv.time"