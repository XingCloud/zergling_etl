create database odin;

CREATE external  table nav_visit (
  pid string,
  time timestamp,
  reqid string,
  uid string,
  ip string,
  nation string,
  ua string,
  os string,
  width string,
  height string,
  refer string)
 partitioned by(day string)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

CREATE external  table search (
  pid string,
  time timestamp,
  reqid string,
  uid string,
  nation string,
  ua string,
  keyword string)
 partitioned by(day string)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

CREATE external table ad_impression(
  time timestamp,
  reqid string,
  uid string,
  slot string,
  adid string,
  adtype string,
  pid string)
 partitioned by(day string)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

#time uid ip nation lang os ptid title meta url
CREATE external table gdp(
  time timestamp,
  uid string,
  ip string,
  nation string,
  lang string,
  os string,
  ptid string,
  title string,
  meta string,
  url string)
 partitioned by(day string)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
