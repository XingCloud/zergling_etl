package com.elex.bigdata.zergling.etl.hbase;

import com.elex.bigdata.zergling.etl.ETLConstants;
import com.elex.bigdata.zergling.etl.ETLUtils;
import com.elex.bigdata.zergling.etl.model.LogType;
import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AresClickHBaseBuilder implements HBaseBuilder {

    public static final Log LOG = LogFactory.getLog(LogType.ARES.getType());
    public static final Log Ares_LOG = LogFactory.getLog("ares");

    //ares_convs
    private byte[] cf = Bytes.toBytes("c");
    private byte[] uidCol = Bytes.toBytes("uid");
    private byte[] reqidCol = Bytes.toBytes("reqid");
    private byte[] nationCol = Bytes.toBytes("na");
    private byte[] pidCol = Bytes.toBytes("pid");
    private byte[] netWrokCol = Bytes.toBytes("nw");
    private byte[] campIDCol = Bytes.toBytes("camp");
    private byte[] adIDCol = Bytes.toBytes("ad");
    private byte[] monekeyCol = Bytes.toBytes("mk");
    private byte[] sub1Col = Bytes.toBytes("sub1");
    private byte[] sub2Col = Bytes.toBytes("sub2");
    private byte[] categoryCol = Bytes.toBytes("cat");
    private byte[] titleCol = Bytes.toBytes("title");

    private Gson gson = new Gson();

    public AresClickHBaseBuilder(){
    }

    @Override
    public Put buildPut(String line) throws Exception {
        Map click = gson.fromJson(line, Map.class);
        String reqid = click.get("reqid") == null ? "NA" : click.get("reqid").toString();
        String uid = click.get("uid") == null ? "NA" : click.get("uid").toString();

        String nation = "xx";
        if(click.get("nation") != null && click.get("nation").toString().length() == 2 ){
            nation = click.get("nation").toString().toLowerCase();
        }
        String pid = "xx";
        if(click.get("src") == null && click.get("site") != null && "blank".equals(click.get("site"))){
            pid = click.get("site").toString().split("\\.")[1];
        }else if(click.get("src") != null){
            pid = click.get("src").toString();
        }

        String category = click.get("category").toString();
        String network = click.get("network").toString();
        String campID = click.get("id").toString();

        long timestamp = ((Double)click.get("ts")).longValue();

        byte[] rowKey = Bytes.toBytes(click.get("clickid").toString());
        Put put = new Put(rowKey);
        put.add(cf, uidCol, timestamp, Bytes.toBytes(uid));
        put.add(cf, reqidCol, timestamp, Bytes.toBytes(reqid));
        put.add(cf, nationCol, timestamp, Bytes.toBytes(nation));
        put.add(cf, pidCol, timestamp, Bytes.toBytes(pid));
        put.add(cf, campIDCol, timestamp, Bytes.toBytes(campID));
        put.add(cf, netWrokCol, timestamp, Bytes.toBytes(network));
        put.add(cf, categoryCol, timestamp, Bytes.toBytes(category));
        putNotNull(put, cf, adIDCol, timestamp, click.get("ad"));
        putNotNull(put, cf, titleCol, timestamp, click.get("title"));
        putNotNull(put, cf, monekeyCol, timestamp, click.get("monkey"));
        putNotNull(put, cf, sub1Col,  timestamp, click.get("sub1"));
        putNotNull(put, cf, sub2Col,  timestamp, click.get("sub2"));

        return put;
    }

    @Override
    public void cleanup() throws Exception {
    }

    private void putNotNull(Put put,byte[] cf,byte[] col,long time ,Object value){
        if(value != null){
            put.add(cf,col,time,Bytes.toBytes(value.toString()));
        }
    }

}
