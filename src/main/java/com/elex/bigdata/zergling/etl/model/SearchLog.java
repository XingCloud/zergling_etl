package com.elex.bigdata.zergling.etl.model;


import org.apache.hadoop.hbase.util.Bytes;

/**
 * Author: liqiang
 * Date: 14-3-18
 * Time: 下午3:29
 */
public class SearchLog {

    private static final String CF1 ="basis"; //family
    private static final String CF2 = "extend"; //family

    private String ts;
    private String nation;
    private String uid;

    @BigDataColumn(cf = CF1, q = "query")
    private String query;
    @BigDataColumn(cf = CF1, q = "ip")
    private long ip;
    @BigDataColumn(cf = CF1, q = "s")
    private String service;

    @BigDataColumn(cf = CF2, q = "vt")
    private String visitTime;
    @BigDataColumn(cf = CF2, q = "vc")
    private String visitCounter;
    @BigDataColumn(cf = CF2, q = "uri")
    private String uri;
    @BigDataColumn(cf = CF2, q = "ua")
    private String ua;
    @BigDataColumn(cf = CF2, q = "hr")
    private String httpReferer;

    public SearchLog(String ts,String nation,String uid, String query, long ip, String service, String visitTime,String visitCounter,String uri,String ua, String httpReferer){
        this.ts = ts;
        this.nation = nation;
        this.uid = uid;
        this.query = query;
        this.ip = ip;
        this.service = service;
        this.visitTime = visitTime;
        this.visitCounter = visitCounter;
        this.uri = uri;
        this.ua = ua;
        this.httpReferer = httpReferer;
    }

    public byte[] getRowkey() {
        return Bytes.toBytes(ts.concat(nation).concat(uid));
    }

    public static String getCf1() {
        return CF1;
    }

    public static String getCf2() {
        return CF2;
    }

    public String getQuery() {
        return query;
    }

    public long getIp() {
        return ip;
    }

    public String getService() {
        return service;
    }

    public String getVisitTime() {
        return visitTime;
    }

    public String getVisitCounter() {
        return visitCounter;
    }

    public String getUri() {
        return uri;
    }

    public String getUa() {
        return ua;
    }

    public String getHttpReferer() {
        return httpReferer;
    }

    @Override
    public String toString() {
        return "SearchLog{" +
                "ts='" + ts + '\'' +
                ", nation='" + nation + '\'' +
                ", uid='" + uid + '\'' +
                ", query='" + query + '\'' +
                ", ip=" + ip +
                ", service='" + service + '\'' +
                ", visitTime='" + visitTime + '\'' +
                ", visitCounter='" + visitCounter + '\'' +
                ", uri='" + uri + '\'' +
                ", ua='" + ua + '\'' +
                ", httpReferer='" + httpReferer + '\'' +
                '}';
    }
}
