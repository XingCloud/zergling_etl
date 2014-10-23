
if [ $# = 1 ] ; then
    day=$1
else
    day=`date +%Y%m%d`
fi

for project in nv search ad_imp gdp ac
do
    hadoop fs -mkdir odin/$project/$day
done

hive -e " use odin;
    alter table nav_visit add partition(day='$day') location '/user/hadoop/odin/nv/$day/' ;
    alter table search add partition(day='$day') location '/user/hadoop/odin/search/$day/' ;
    alter table ad_impression add partition(day='$day') location '/user/hadoop/odin/ad_imp/$day/';
    alter table gdp add partition(day='$day') location '/user/hadoop/odin/gdp/$day/';
    alter table ad_click add partition(day='$day') location '/user/hadoop/odin/ac/$day/'; "


