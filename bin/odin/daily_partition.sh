
day=`date +%Y%m%d`

for project in nv search ad_imp
do
    hadoop fs -mkdir odin/$project/$day
done

hive -e "use odin;
    alter table nav_visit add partition(day='$day') location '/user/hadoop/odin/nv/$day/' ;
    alter table search add partition(day='$day') location '/user/hadoop/odin/search/$day/' ;
    alter table ad_impression add partition(day='$day') location '/user/hadoop/odin/ad_imp/$day/' "


