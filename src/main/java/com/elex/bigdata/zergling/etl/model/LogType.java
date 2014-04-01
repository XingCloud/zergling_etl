package com.elex.bigdata.zergling.etl.model;

import com.elex.bigdata.zergling.etl.hbase.ADLogHBaseBuilder;
import com.elex.bigdata.zergling.etl.hbase.HBaseBuilder;
import com.elex.bigdata.zergling.etl.hbase.SearchLogHBaseBuilder;

/**
 * Author: liqiang
 * Date: 14-3-17
 * Time: 下午4:37
 */
public enum LogType {

    SEARCH("search"){
        @Override
        public HBaseBuilder getBuilder() {
            return new SearchLogHBaseBuilder();
        }
    },AD("ad"){
        @Override
        public HBaseBuilder getBuilder() {
            return new ADLogHBaseBuilder();
        }
    };

    private LogType(String type){
        this.type = type;
    }

    private final String type;

    public abstract HBaseBuilder getBuilder();

    public String getType(){
        return this.type;
    }

    public static LogType getLogType(String type){
        for(LogType logType : LogType.values()){
            if(logType.getType().equals(type)){
                return logType;
            }
        }
        return null;
    }
}
