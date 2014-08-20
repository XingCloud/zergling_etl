package com.elex.bigdata.cp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: liqiang
 * 用于在广告点击的时候补入cookieid 和 uid的映射关系
 * 一个UID可能对应多个COOKIEID （cookie失效的情况下）
 * 一个COOKIEID可能对应多个UID (多个项目)
 * 为了避免存的版本过多，同一个映射关系只存一次
 * Date: 14-8-14
 * Time: 下午4:03
 */
public class ADCookieMapping extends BaseRegionObserver{

    private HConnection conn = null;
    private final static String  INDEX_TABLE  = "cookie_uid_map";

    public static byte[] adcf = Bytes.toBytes("basis"); //AD的表family
    public static byte[] cuf = Bytes.toBytes("cu");
    public static byte[] uidCol = Bytes.toBytes("uid");
    public static byte[] projectCol = Bytes.toBytes("p");
    public static byte[] cookie_prefix = Bytes.toBytes("c_"); //key: c_cookie, value: uid
    public static byte[] uid_prefix = Bytes.toBytes("u_"); //key: u_uid, value: cookieid
    public static byte[] mix_prefix = Bytes.toBytes("m_"); //HASH COOKIE+UID+P ,用来判断映射关系是否已经存在

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        conn = HConnectionManager.createConnection(env.getConfiguration());
    }


    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e,
                        final Put put, final WALEdit edit, final boolean writeToWAL) throws IOException {
        if(put.has(adcf,uidCol)){

            long time = put.get(adcf, uidCol).get(0).getTimestamp();

            byte[] cookie = Bytes.tail(put.getRow(), put.getRow().length - 11);
            byte[] uid = put.get(adcf,uidCol).get(0).getValue();

            byte[] p = Bytes.toBytes(String.valueOf((int) put.getRow()[0]));

            byte[] cookieRow = Bytes.add(cookie_prefix,cookie);
            byte[] uidRow = Bytes.add(uid_prefix,uid);

            //防止key过长，做MD5
            byte[] mixID = Bytes.add(p,cookie,uid);
            String md5Hash = MD5Hash.getMD5AsHex(mixID, 0, mixID.length);
            byte [] md5HashBytes = Bytes.toBytes(md5Hash);

            byte[] mixRow = Bytes.add(mix_prefix,md5HashBytes);


            HTable table = (HTable)conn.getTable(INDEX_TABLE);
            Put cuPut = new Put(cookieRow);
            cuPut.add(cuf,uidCol,time,uid);
            cuPut.add(cuf,projectCol,time,p);

            Put ucPut = new Put(uidRow);
            ucPut.add(cuf,uidCol,time,cookie);
            ucPut.add(cuf,projectCol,time,p);

            Put mixPut = new Put(mixRow);
            mixPut.add(cuf,projectCol,p);

            if(!table.exists(new Get(mixRow))){
                List<Put> puts = new ArrayList<Put>();
                puts.add(cuPut);
                puts.add(ucPut);
                puts.add(mixPut);
                table.put(puts);
            }

            table.close();
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        conn.close();
    }
}
