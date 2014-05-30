package com.elex.bigdata.zergling.etl.hbase;

import com.elex.bigdata.zergling.etl.model.LogType;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Author: liqiang
 * Date: 14-3-17
 * Time: 下午2:18
 */
public class HBasePutterV2 implements Callable<String> {

    private static Logger LOGGER = null;
    private static Logger IGNORE_LOGGER = null;
    private HBaseBuilder builder;
    private List<String> lines;
    private String tableName;
    private AtomicLong counter;

    public HBasePutterV2(String logType, HBaseBuilder builder, String tableName,List<String> lines,AtomicLong counter) throws Exception {
        LOGGER = Logger.getLogger(logType);
        IGNORE_LOGGER = Logger.getLogger(logType + "_ignore");
        this.builder = builder;
        this.tableName = tableName;
        this.lines = lines;
        this.counter = counter;
    }

    @Override
    public String call() throws Exception{
        List<Put> puts = new ArrayList<Put>();
        long begin = System.currentTimeMillis();
        for(String line: lines){
            try {
                puts.add(builder.buildPut(line));
            } catch (Exception e) {
                IGNORE_LOGGER.info(line);
                LOGGER.warn("get exception:" + e.getMessage() + ", ignore the log : " + line);
                e.printStackTrace();
            }
        }
        if(puts.size()>0){
            HTableInterface hTable = null;
            try {
                hTable = HBaseResourceManager.getHTable(tableName);
                hTable.put(puts);
                this.counter.addAndGet(puts.size());
            } catch (Exception e) {
                //此时的失败一般是因为HBase出问题了，暂不分离日志
                LOGGER.error("Fail to insert data after spend "+(System.currentTimeMillis() - begin ) +"ms as ：" + e.getMessage() );
                throw e;
            } finally {
                HBaseResourceManager.closeHTable(hTable);
            }
        }
        LOGGER.info("Insert " + puts.size() + " logs spend " + (System.currentTimeMillis() - begin ) + "ms");
        return "success";
    }
}
