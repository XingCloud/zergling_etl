package com.elex.bigdata.cp;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class AresClickIndex extends BaseRegionObserver{

    private HConnection conn = null;
    private final static String  INDEX_TABLE  = "ares_click_idx";

    public static byte[] adcf = Bytes.toBytes("c"); //AD的表family
    public static byte[] dumyC = Bytes.toBytes("c");
    public static byte[] nationCol = Bytes.toBytes("na");
    public static byte[] projectCol = Bytes.toBytes("pid");
    public static byte[] campIDCol = Bytes.toBytes("camp");
    public static byte[] adIDCol = Bytes.toBytes("ad");

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        conn = HConnectionManager.createConnection(env.getConfiguration());
    }


    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        conn.close();
    }

    /*    ares_click_idx
    key1: 1 + day + clickid
    key2: 2 + day + nation + clickid
    key3: 3 + day + pid + clickid
    cf : c
    col: campid adid */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        if(put.has(adcf,nationCol)){

            long time = put.get(adcf, nationCol).get(0).getTimestamp();
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(time);

            int month = c.get(Calendar.MONTH) + 1;
            String monStr = String.valueOf(month);
            if(month < 10){
                monStr = "0" + monStr;
            }

            int date = c.get(Calendar.DATE);
            String dateStr = String.valueOf(date);
            if(date <10){
                dateStr = "0" + dateStr;
            }

            String day = String.valueOf(c.get(Calendar.YEAR)) + monStr + dateStr;

            byte[] key1 = Bytes.add(Bytes.toBytes("1" + day), put.getRow());
            byte[] key2 = Bytes.add(Bytes.toBytes("2" + day), put.get(adcf, projectCol).get(0).getValue(), put.getRow());
            byte[] key3 = Bytes.add(Bytes.toBytes("3" + day), put.get(adcf, nationCol).get(0).getValue(), put.getRow());

            byte[] cmpID = put.get(adcf, campIDCol).get(0).getValue();
            byte[] adID = null;

            if(put.has(adcf, adIDCol)){
                adID = put.get(adcf, adIDCol).get(0).getValue();
            }

            HTable table = (HTable)conn.getTable(INDEX_TABLE);
            Put put1 = new Put(key1);
            put1.add(adcf,campIDCol,time,cmpID);

            Put put2 = new Put(key2);
            put2.add(adcf,campIDCol,time,cmpID);

            Put put3 = new Put(key3);
            put3.add(adcf,campIDCol,time,cmpID);

            if(adID != null){
                put1.add(adcf, adIDCol, time, adID);
                put2.add(adcf, adIDCol, time, adID);
                put3.add(adcf, adIDCol, time, adID);
            }

            List<Put> puts = new ArrayList<Put>();
            puts.add(put1);
            puts.add(put2);
            puts.add(put3);
            table.put(puts);

            table.close();
        }
    }
}
