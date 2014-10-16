
day=`date +%Y%m%d`

for project in nv search ad_imp
do
    hadoop fs -mkdir odin/$project/$day
done

hive -e "
    alter table odin.nav_visit add partition(day='$day') location '/user/hadoop/odin/nv/$day/' ;
    alter table odin.search add partition(day='$day') location '/user/hadoop/odin/search/$day/' ;
    alter table odin.ad_impression add partition(day='$day') location '/user/hadoop/odin/ad_imp/$day/' "


