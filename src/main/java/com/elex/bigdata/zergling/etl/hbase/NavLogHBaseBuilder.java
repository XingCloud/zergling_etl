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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Author: liqiang
 * Date: 14-3-25
 * Time: 上午9:27
 */
public class NavLogHBaseBuilder implements HBaseBuilder {

    public static final Log LOG = LogFactory.getLog(NavLogHBaseBuilder.class);
    public static MetricMapping metricMapping = MetricMapping.getInstance();
    private byte[] cf = Bytes.toBytes("basis");
    private byte[] urlCol = Bytes.toBytes("url");
    private byte[] ipCol = Bytes.toBytes("ip");
    private byte[] areaCol = Bytes.toBytes("area");
    private byte[] reqidCol = Bytes.toBytes("reqid");
    private String urlPreffix = "/nav.png?";
    private static final String COMBINE_NATION_SEPRATOR = "_";
    private ThreadLocal<SimpleDateFormat> sdf = new ThreadLocal<SimpleDateFormat>(){
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        }
    };
    private static final String LOG_ATTR_SEPRATOR = "\t";
    private static final String LOG_URI_SEPRATOR = "&";
    private static final String LOG_URI_PARAM_SEPRATOR = "=";

    private static Set<String> historyNations;
    private static ConcurrentHashMap<String,String> newNations = new ConcurrentHashMap<String,String>();

    public NavLogHBaseBuilder(){
        MongoDriver.getInstance();
        historyNations = MetricMapping.getAllNationsAsSet();
    }

    @Override
    public Put buildPut(String line) throws Exception {
        //        /nav.png?p=awesomehp&nation=br&uid=WDCXWD3200AAJS-00YZCA0_WD-WCAYU264665946659&url=http://pt-br.facebook.com/
        List<String> attrs = ETLUtils.split(line,LOG_ATTR_SEPRATOR);
        List<String> uriParams = ETLUtils.split(attrs.get(2).substring(urlPreffix.length()),LOG_URI_SEPRATOR);

        Map<String,String> params = new HashMap<String,String>();
        for(String param : uriParams){
            int pos = param.indexOf(LOG_URI_PARAM_SEPRATOR);
            String key = param.substring(0, pos);
            String value = param.substring(pos + 1);
            params.put(key,value);
        }

        if(StringUtils.isBlank(params.get("p")) || StringUtils.isBlank(params.get("nation"))
                || StringUtils.isBlank(params.get("uid")) || StringUtils.isBlank(params.get("url"))){
            throw new Exception(" One ad params is null");
        }

        long time = sdf.get().parse(attrs.get(1)).getTime();

        //2014-10-14T23:55:03+08:00
        String date = fillDate(attrs.get(1));
        String ip = attrs.get(0).split(", ")[0];

        //PID + NATION + TIME + UID
        Byte pid = metricMapping.getProjectURLByte(params.get("p"));
        if(pid == null){
            throw new Exception("Could not find the mapped pid for URL:" + params.get("p"));
        }
        String nation = params.get("nation").toLowerCase();
        byte[] rowKeyPreffix = Bytes.add(new byte[]{pid.byteValue()}, Bytes.toBytes(nation), Bytes.toBytes(date));
        byte[] rowKey = Bytes.add(rowKeyPreffix, Bytes.toBytes(params.get("uid")));

        Put put = new Put(rowKey);
        put.add(cf,urlCol,time,Bytes.toBytes(params.get("url")));
        put.add(cf,ipCol,time,Bytes.toBytes(ip));
        putNotNull(put, cf, areaCol, time, params.get("area"));
        putNotNull(put, cf, reqidCol, time, params.get("reqID"));

        //更新nation,nation放到mongo中，方便训练的时候使用
        try{
            String comNation = pid.toString() + COMBINE_NATION_SEPRATOR + nation;
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

    private void putNotNull(Put put,byte[] cf,byte[] col,long time ,String value){
        if(value != null){
            put.add(cf,col,time,Bytes.toBytes(value));
        }
    }

    private String fillDate(String dateString) throws Exception {
        try {
            char[] dateChars = new char[14];
            dateChars[0] = dateString.charAt(0);
            dateChars[1] = dateString.charAt(1);
            dateChars[2] = dateString.charAt(2);
            dateChars[3] = dateString.charAt(3);
            dateChars[4] = dateString.charAt(5);
            dateChars[5] = dateString.charAt(6);
            dateChars[6] = dateString.charAt(8);
            dateChars[7] = dateString.charAt(9);
            dateChars[8] = dateString.charAt(11);
            dateChars[9] = dateString.charAt(12);
            dateChars[10] = dateString.charAt(14);
            dateChars[11] = dateString.charAt(15);
            dateChars[12] = dateString.charAt(17);
            dateChars[13] = dateString.charAt(18);
            return new String(dateChars);
        } catch (Exception e) {
            throw new Exception("Cannot fill date(" + dateString + ")");
        }
    }
}
