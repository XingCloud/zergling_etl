package com.elex.bigdata.zergling.etl.hbase;

import com.elex.bigdata.zergling.etl.ETLUtils;
import com.elex.bigdata.zergling.etl.model.LogType;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class YACLogHBaseBuilder implements HBaseBuilder {

    public static final Log LOG = LogFactory.getLog(LogType.YAC.getType());

    //yac_user_action
    private byte[] ucf = Bytes.toBytes("ua");
    private byte[] actionCol = Bytes.toBytes("a");
    private byte[] urlCol = Bytes.toBytes("url");
    private byte[] durationCol = Bytes.toBytes("d");
    private byte[] paCol = Bytes.toBytes("pa");
    private byte[] projectCol = Bytes.toBytes("p");
    private byte[] ipCol = Bytes.toBytes("ip");
    private byte[] nationCol = Bytes.toBytes("nt");

    //dmp_url_detail
    private String urlDetailTableName = "dmp_url_detail";
    private byte[] dcf = Bytes.toBytes("ud");
    private byte[] titleCol = Bytes.toBytes("t");
    private byte[] metaCol = Bytes.toBytes("m");
    private byte[] langCol = Bytes.toBytes("l");
    private byte[] drowPrefix = Bytes.toBytes("F");

    private static final String LOG_ATTR_SEPRATOR = "\t";
    private static Random random = new Random();

    public YACLogHBaseBuilder(){
    }

    @Override
    public Put buildPut(String line) throws Exception {

        //uid  ip nation ts url title 网站语言 metainfo 停留时间
        List<String> attrs = ETLUtils.split(line, LOG_ATTR_SEPRATOR);

        if(attrs.size() < 9){
            throw new Exception("Yac log params size less than 9");
        }

        if(!ETLUtils.validateURL(attrs.get(4))){
            throw new Exception("The URL is invalid");
        }

        //时间只有10位，手动加上3位随机数
        int randomTime = random.nextInt(999);
        String timeSuffix = String.valueOf(randomTime);
        if(randomTime == 0){
            timeSuffix = "000";
        }if(randomTime<10){
            timeSuffix = "00" + randomTime;
        }else if(randomTime<100){
            timeSuffix = "0" + randomTime;
        }
        long time = Long.parseLong(attrs.get(3) + timeSuffix);

        //添加到URL字典表
//        putURLDetail(attrs,time);

        long ip = 0;
        if(StringUtils.isNotBlank(attrs.get(1))){
            ip = ETLUtils.ip2Long(attrs.get(1));
        }

        UrlType urlType = UrlType.CLICK; //0:所有tab;1:跳转;2:页面停留时间
        int duration = 0;
        if(attrs.get(8).length() > 0){
            urlType = UrlType.DURATION;
            duration = Integer.parseInt(attrs.get(8));
        }

        String nation = attrs.get(2).toLowerCase();
        byte[] rowKey = Bytes.add(new byte[]{(byte) urlType.getType()}, Bytes.toBytes(time), Bytes.toBytes(attrs.get(0)));

        Put put = new Put(rowKey);
        put.add(ucf,ipCol,time,Bytes.toBytes(ip));
        put.add(ucf,nationCol,time,Bytes.toBytes(nation));

        if(urlType == urlType.DURATION){
            put.add(ucf,durationCol,time,Bytes.toBytes(duration));
        }

        put.add(ucf,urlCol,time,Bytes.toBytes(attrs.get(4)));
        return put;
    }

    @Override
    public void cleanup() throws Exception {
    }

    private enum UrlType {

        CLICK(1),DURATION(2);

        private UrlType(int type){
            this.type = type;
        }
        private final int type;
        public int getType(){
            return this.type;
        }
    }

    private void putURLDetail(List<String> content,long time) throws Exception {
        List<Put> urlDetailPuts = new ArrayList<Put>();
        try{
            //uid  ip nation ts url title 网站语言 metainfo 停留时间
            Put put = new Put(Bytes.add(drowPrefix,Bytes.toBytes(content.get(4))));
            put.add(dcf,titleCol,time,Bytes.toBytes(content.get(5)));
            put.add(dcf,langCol,time,Bytes.toBytes(content.get(6)));
            put.add(dcf,metaCol,time,Bytes.toBytes(content.get(7)));

            urlDetailPuts.add(put);
        }catch (Exception e){
            throw new Exception("Error when parse the url detail",e.getCause());
        }

        HBaseSimpleAPI.put(urlDetailTableName,urlDetailPuts);
    }
}
