package com.elex.bigdata.zergling.etl.hbase;

import com.elex.bigdata.util.MetricMapping;
import com.elex.bigdata.zergling.etl.ETLUtils;
import com.elex.bigdata.zergling.etl.driver.MongoDriver;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.bson.BSONObject;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Author: liqiang
 * Date: 14-3-25
 * Time: 上午9:27
 */
public class ADLogHBaseBuilder implements HBaseBuilder {

    public static final Log LOG = LogFactory.getLog(ADLogHBaseBuilder.class);
    public static MetricMapping metricMapping = MetricMapping.getInstance();
    private byte[] cf = Bytes.toBytes("basis");
    private byte[] scoreCf = Bytes.toBytes("h"); //hit 命中情况
    private byte[] aidCol = Bytes.toBytes("aid");
    private byte[] ipCol = Bytes.toBytes("ip");
    private byte[] urlCol = Bytes.toBytes("url");
    private byte[] cpcCol = Bytes.toBytes("cpc");
    private byte[] categoryCol = Bytes.toBytes("c");
    private byte[] widthCol = Bytes.toBytes("w");
    private byte[] heightCol = Bytes.toBytes("h");
    private String urlPreffix = "/ad.png?";
    private ThreadLocal<SimpleDateFormat> sdf = new ThreadLocal<SimpleDateFormat>(){
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        }
    };
//    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    private Map<String,byte[]> adDetailKeys = new HashMap<String, byte[]>();
    private static final String LOG_ATTR_SEPRATOR = "\t";
    private static final String LOG_URI_SEPRATOR = "&";
    private static final String LOG_URI_PARAM_SEPRATOR = "=";
    private static final String LOG_HIT_SEPRATOR = ",";
    private static final String LOG_HIT_KV_SEPRATOR = ".";
    private static final String COMBINE_NATION_SEPRATOR = "_";
    private static Set<String> historyNations;
    private static ConcurrentHashMap<String,String> newNations = new ConcurrentHashMap<String,String>();

    public ADLogHBaseBuilder(){
        MongoDriver.getInstance();
        adDetailKeys.put("w",widthCol);
        adDetailKeys.put("h",heightCol);
        adDetailKeys.put("category",categoryCol);
        adDetailKeys.put("cpc",cpcCol);
        adDetailKeys.put("url",urlCol);

        historyNations = MetricMapping.getAllNationsAsSet();
    }

    @Override
    public Put buildPut(String line) throws Exception {
        //        0  10.1.20.152
        //        1  2014-03-31T14:24:23+08:00
        //        2  /ad.png?p=www.awesomehp.com&ip=37.239.46.2&nation=IQ&uid=WDCXWD5000BPVT-22HXZT3_WD-WXL1A911029910299&aid=1883&t=

        List<String> attrs = split(line,LOG_ATTR_SEPRATOR);
        List<String> uriParams = split(attrs.get(2).substring(urlPreffix.length()),LOG_URI_SEPRATOR);

        Map<String,String> params = new HashMap<String,String>();
        for(String param : uriParams){
            int pos = param.indexOf(LOG_URI_PARAM_SEPRATOR);
            String key = param.substring(0, pos);
            String value = param.substring(pos + 1);
            params.put(key,value);
        }

        if(StringUtils.isBlank(params.get("p")) || StringUtils.isBlank(params.get("nation"))
                || StringUtils.isBlank(params.get("uid")) || StringUtils.isBlank(params.get("aid"))
                || StringUtils.isBlank(params.get("ip")) ){
            throw new Exception(" One ad params is null");
        }

        //日志明细
        BSONObject adDetail = MongoDriver.getADDetail(params.get("aid"));
        if(adDetail == null){
            throw new Exception("Could not find the AD detail for aid " + params.get("aid"));
        }

        long ip = ETLUtils.ip2Long(params.get("ip"));
        long time = sdf.get().parse(attrs.get(1)).getTime();

        //PID + NATION + TIME + UID
        Byte pid = metricMapping.getProjectURLByte(params.get("p"));
        if(pid == null){
            throw new Exception("Could not find the mapped pid for URL:" + params.get("p"));
        }
        byte[] rowKeyPreffix = Bytes.add(new byte[]{pid.byteValue()},Bytes.toBytes(params.get("nation")),Bytes.toBytes(time));
        byte[] rowKey = Bytes.add(rowKeyPreffix, Bytes.toBytes(params.get("uid")));

        Put put = new Put(rowKey);
        put.add(cf,aidCol,time,Bytes.toBytes(params.get("aid")));
        put.add(cf,ipCol,time,Bytes.toBytes(ip));

        //广告明细
        for(Map.Entry<String,byte[]> adDetailKey : adDetailKeys.entrySet()){
            Object value = adDetail.get(adDetailKey.getKey());
            if(value != null){
                put.add(cf,adDetailKey.getValue(),time,Bytes.toBytes(String.valueOf(value)));
            }
        }

        //命中情况
        if(StringUtils.isNotBlank(params.get("t"))){
            List<String> scores = split(params.get("t"),LOG_HIT_SEPRATOR);
            //b.19,a.21,z.60 a.游戏 b.电商 z.未知
            for(String score : scores){
                int pos = score.indexOf(LOG_HIT_KV_SEPRATOR);
                String key = score.substring(0, pos);
                Integer value = Integer.parseInt(score.substring(pos + 1));
                put.add(scoreCf,Bytes.toBytes(key),time,Bytes.toBytes(value));
            }
            //0，未指定 1，游戏 2，电商 99，其他 ,冗余存一份，用于后期指定CF分析命中情况使用
            //具体是否命中不在这里计算，后期可能类型会有更新，这里不易枚举映射情况
            Object cat = adDetail.get("category");
            put.add(scoreCf,categoryCol,time,Bytes.toBytes(Integer.parseInt(cat.toString())));
        }

        //更新nation,nation放到mongo中，方便训练的时候使用
        try{
            String comNation = pid.toString() + COMBINE_NATION_SEPRATOR + params.get("nation");
            if(!historyNations.contains(comNation) && newNations.get(comNation) == null){
                newNations.put(comNation,"");
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        return put;
    }

    @Override
    public void cleanup() throws Exception {
        MetricMapping.insertNations(newNations.keySet());
    }


    private List<String> split(String line, String sep){
        List<String> attrs = new ArrayList<String>();
        int pos = 0, end;
        while ((end = line.indexOf(sep, pos)) >= 0) {
            attrs.add(line.substring(pos, end));
            pos = end + 1;
        }
        attrs.add(line.substring(pos)); //最后一个
        return attrs;
    }

}
