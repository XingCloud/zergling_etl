package com.elex.bigdata.zergling.etl.hbase;

import com.elex.bigdata.hashing.BKDRHash;
import com.elex.bigdata.util.MetricMapping;
import com.elex.bigdata.zergling.etl.ETLUtils;
import com.elex.bigdata.zergling.etl.driver.MongoDriver;
import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class PluginLogHBaseBuilder implements HBaseBuilder {

    public static final Log LOG = LogFactory.getLog(PluginLogHBaseBuilder.class);

    //dmp_user_action
    private byte[] ucf = Bytes.toBytes("ua");
    private byte[] actionCol = Bytes.toBytes("a");
    private byte[] urlCol = Bytes.toBytes("url");
    private byte[] durationCol = Bytes.toBytes("d");
    private byte[] paCol = Bytes.toBytes("pa");
    private byte[] projectCol = Bytes.toBytes("p");
    private byte[] catCol = Bytes.toBytes("cat");
    private byte[] ipCol = Bytes.toBytes("ip");
    private byte[] nationCol = Bytes.toBytes("nt");

    //dmp_url_detail
    private String urlDetailTableName = "dmp_url_detail";
    private byte[] dcf = Bytes.toBytes("ud");
    private byte[] titleCol = Bytes.toBytes("t");
    private byte[] metaCol = Bytes.toBytes("m");
    private byte[] langCol = Bytes.toBytes("l");
    private byte[] drowPrefix = Bytes.toBytes("F");

    private String urlPreffix = "/pc.png?";
    private Gson gson = new Gson();

    private static final String LOG_ATTR_SEPRATOR = "\t";
    private static final String LOG_URI_SEPRATOR = "&";
    private static final String LOG_URI_PARAM_SEPRATOR = "=";

    public PluginLogHBaseBuilder(){
    }

    @Override
    public Put buildPut(String line) throws Exception {
        //        0  10.1.20.152
        //        1  2014-03-31T14:24:23+08:00
        //        2  /pc.png?nation=us&ip=&action=click&category=0&uts=1401418227487&uid=395049983_1052515_989BEF9B&content=[[%22Google%22,%22http://www.google.com/%22,%22%22,%22%22,%22%22,%22us%22]]

        //将content分开，里面有特殊字符
        List<String> sepLines = split(line,"&content=");
        List<String> attrs = split(sepLines.get(0),LOG_ATTR_SEPRATOR);
        List<String> uriParams = split(attrs.get(2).substring(urlPreffix.length()),LOG_URI_SEPRATOR);

        Map<String,String> params = new HashMap<String,String>();
        for(String param : uriParams){
            int pos = param.indexOf(LOG_URI_PARAM_SEPRATOR);
            String key = param.substring(0, pos);
            String value = param.substring(pos + 1);
            params.put(key,value);
        }

        if(StringUtils.isBlank(params.get("uts")) || StringUtils.isBlank(params.get("nation"))
                || StringUtils.isBlank(params.get("uid")) || StringUtils.isBlank(params.get("action"))
                || StringUtils.isBlank(attrs.get(2)) ){
            throw new Exception(" One pc params is null");
        }

        String[][] content = gson.fromJson(URLDecoder.decode(sepLines.get(1),"utf-8"), String[][].class);
        long time = Long.parseLong(params.get("uts"));

        //添加到URL字典表
        int[] hashUrls = new int[content.length];
        for(int i=0;i<content.length;i++){
            hashUrls[i] = putURLDetail(content[i],time);
        }

        long ip = 0;
        if(StringUtils.isNotBlank(params.get("ip"))){
            ip = ETLUtils.ip2Long(params.get("ip"));
        }

        PluginType pluginType = PluginType.TAB; //0:所有tab;1:跳转;2:页面停留时间
        int duration = 0;
        if(content.length == 1){
            if(StringUtils.isNotBlank(params.get("time"))){
                pluginType = PluginType.DURATION;
                duration = Integer.parseInt(params.get("time"));
            }else{
                pluginType =PluginType.CLICK;
            }
        }

        int category = -1;
        if(StringUtils.isNotBlank(params.get("category"))){
            category = Integer.parseInt(params.get("category"));
        }

        String nation = params.get("nation").toLowerCase();
        byte[] rowKey = Bytes.add(new byte[]{(byte)pluginType.getType()},Bytes.toBytes(time),Bytes.toBytes(params.get("uid")));

        Put put = new Put(rowKey);
        put.add(ucf,actionCol,time,Bytes.toBytes(params.get("action")));
        put.add(ucf,ipCol,time,Bytes.toBytes(ip));
        put.add(ucf,nationCol,time,Bytes.toBytes(nation));

        if(pluginType != PluginType.TAB){

            if(pluginType == PluginType.DURATION){
                put.add(ucf,durationCol,time,Bytes.toBytes(duration));
            }

            put.add(ucf,urlCol,time,Bytes.toBytes(hashUrls[0]));
            put.add(ucf,catCol,time,Bytes.toBytes(category));
            put.add(ucf,paCol,time,Bytes.toBytes(content[0][4]));
            put.add(ucf,projectCol,time,Bytes.toBytes(content[0][5]));
        }else{
            String strHashUrl = "" + hashUrls[0];
            for(int i =1;i<hashUrls.length;i++){
                strHashUrl += "," + hashUrls[i];
            }
            put.add(ucf,urlCol,time,Bytes.toBytes(strHashUrl));
        }

        return put;
    }

    @Override
    public void cleanup() throws Exception {
    }


    private List<String> split(String line, String sep){
        List<String> attrs = new ArrayList<String>();
        int pos = 0, end;
        while ((end = line.indexOf(sep, pos)) >= 0) {
            attrs.add(line.substring(pos, end));
            pos = end + sep.length();
        }
        attrs.add(line.substring(pos)); //最后一个
        return attrs;
    }


    private enum PluginType {

        TAB(0),CLICK(1),DURATION(2);

        private PluginType(int type){
            this.type = type;
        }
        private final int type;
        public int getType(){
            return this.type;
        }
    }

    private int putURLDetail(String[] detail,long time) throws Exception {
        List<Put> urlDetailPuts = new ArrayList<Put>();
        int hashURL;
        try{
            hashURL = BKDRHash.getIntFromStr(detail[1]);

            Put put = new Put(Bytes.add(drowPrefix,Bytes.toBytes(hashURL)));
            put.add(dcf,titleCol,time,Bytes.toBytes(detail[0]));
            put.add(dcf,urlCol,time,Bytes.toBytes(detail[1]));
            put.add(dcf,langCol,time,Bytes.toBytes(detail[2]));
            put.add(dcf,metaCol,time,Bytes.toBytes(detail[3]));

            urlDetailPuts.add(put);
        }catch (Exception e){
            throw new Exception("Error when parse the url detail",e.getCause());
        }

        HBaseSimpleAPI.put(urlDetailTableName,urlDetailPuts);
        return hashURL;
    }
}
