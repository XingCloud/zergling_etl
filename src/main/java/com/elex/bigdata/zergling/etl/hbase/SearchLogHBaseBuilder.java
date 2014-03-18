package com.elex.bigdata.zergling.etl.hbase;

import com.elex.bigdata.zergling.etl.ETLConstants;
import com.elex.bigdata.zergling.etl.ETLUtils;
import com.elex.bigdata.zergling.etl.model.ColumnInfo;
import com.elex.bigdata.zergling.etl.model.SearchLog;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * Author: liqiang
 * Date: 14-3-17
 * Time: 下午2:14
 */
public class SearchLogHBaseBuilder implements HBaseBuilder {

    @Override
    public Put buildPut(String line) throws Exception {
        //rowkey : uid + time + (nation)?
        //timestamp : ts
        //cf:  basis : fip query service
        //cf:  extend : visit_time visit_counter uri ua http_referer
        SearchLog searchLog = parse22Find(line + "[");
        System.out.println(searchLog);
        Put put = new Put(searchLog.getRowkey());
        List<ColumnInfo> columns = ETLUtils.getColumnInfos(searchLog);
        for(ColumnInfo colInfo : columns){
            put.add(colInfo.getColumnFamilyBytes(),colInfo.getQualifierBytes(),colInfo.getValueBytes());
        }
        return put;
    }

    private SearchLog parse22Find(String line){
        //暂不抽象成对象，目前没有其他地方使用
        List<String> fields = new ArrayList<String>();
        fields.add("errno");
        fields.add("fip");
        fields.add("logId");
        fields.add("uri");
        fields.add("visit_time");
        fields.add("visit_counter");
        fields.add("project_id");
        fields.add("ts");
        fields.add("cookies");
        fields.add("ua");
        fields.add("query_string");
        fields.add("http_referer");
        fields.add("type");
        fields.add("country");
        fields.add("query");
        fields.add("service");
        fields.add("goto");
        fields.add("22find");

        Map<String,String> kv =parseToMap(fields,line);

        String uid = ETLConstants.UNKOWN_UID;
        Matcher match = ETLConstants.UID_PARTTERN.matcher(kv.get("cookies"));
        if(match.find()){
            uid = match.group(1);
        }

        SearchLog searchLog = new SearchLog(kv.get("ts"), kv.get("country"), uid,
                kv.get("query"), ETLUtils.ip2Long(kv.get("fip")), kv.get("service"), kv.get("visit_time"),
                kv.get("visit_counter"), kv.get("uri"), kv.get("ua"), kv.get("http_referer"));

        return searchLog;
    }
    private Map<String,String> parseToMap(List<String> fields,String line){
        Map<String,String> kv = new HashMap<>();
        for(int i=0;i<fields.size()-1;i++){
            try{
                int start = line.indexOf(" " + fields.get(i) + "[") + fields.get(i).length() + 2;
                int end = line.indexOf("] " + fields.get(i+1) + "[");
                String value = line.substring(start,end);
                kv.put(fields.get(i), value);
            }catch(Exception e){
                throw new RuntimeException("Fail to parse the field[" + fields.get(i) + "]");
            }
        }
        return kv;
    }
}
