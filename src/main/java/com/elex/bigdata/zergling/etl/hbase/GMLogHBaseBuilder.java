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
import java.util.*;

public class GMLogHBaseBuilder implements HBaseBuilder {

    public static final Log LOG = LogFactory.getLog(LogType.GM.getType());
    public static final Log LK_LOG = LogFactory.getLog("gm_lk");

    //gm_user_action
    private byte[] ucf = Bytes.toBytes("ua");
    private byte[] gidCol = Bytes.toBytes("gid"); //游戏ID
    private byte[] langCol = Bytes.toBytes("l"); //语言（zh-CN）
    private byte[] nationCol = Bytes.toBytes("na"); //国家
    private byte[] tzCol = Bytes.toBytes("tz"); //时区
    private byte[] vipCol = Bytes.toBytes("v"); //是否为VIP用户
    private byte[] vaCol = Bytes.toBytes("va"); //是否为年费
    private byte[] vpCol = Bytes.toBytes("vp"); //积分
    private byte[] vlCol = Bytes.toBytes("vl"); //VIP等级
    private byte[] gameTypeCol = Bytes.toBytes("gt"); //游戏类别（web,mini）
    private byte[] clCol = Bytes.toBytes("cl"); //新加的语言字段

    //HB
    private byte[] tagCol = Bytes.toBytes("tag"); //游戏类型
    private byte[] titleCol = Bytes.toBytes("title"); //游戏标题

    //down/up
    private byte[] countCol = Bytes.toBytes("c"); //点赞/点踩次数
    private byte[] gsCol = Bytes.toBytes("gs"); //游戏评分

    //share
    private byte[] fsCol = Bytes.toBytes("fs"); //分享累计值

    //pay
    private byte[] cashCol = Bytes.toBytes("cash"); //充值金额

    //PLAY
    private byte[] recTypeCol = Bytes.toBytes("rt"); //推荐类型
    private byte[] recIdxCol = Bytes.toBytes("idx"); //推荐结果索引 （gid 或者 空）

    //LINK
    private byte[] lidCol = Bytes.toBytes("lid"); //uid(cookieID)关联的注册id

    private String urlPreffix = "/gm.png?";

    private static final String LOG_ATTR_SEPRATOR = "\t";
    private static final String LOG_URI_SEPRATOR = "&";
    private static final String LOG_URI_PARAM_SEPRATOR = "=";

    private ThreadLocal<SimpleDateFormat> sdf = new ThreadLocal<SimpleDateFormat>(){
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        }
    };

    public GMLogHBaseBuilder(){
    }

    @Override
    public Put buildPut(String line) throws Exception {
        //        0  10.1.20.152
        //        1  2014-03-31T14:24:23+08:00
        //        2  /gm.png?action=play&appid=337&uid=elex337_60794166&gid=mario_vs_sonic_racing&l=pt-BR&ts=1403279941636&tz=3

        //将content分开，里面有特殊字符
        List<String> attrs = ETLUtils.split(line,LOG_ATTR_SEPRATOR);
        List<String> uriParams = ETLUtils.split(attrs.get(2).substring(urlPreffix.length()),LOG_URI_SEPRATOR);

        Map<String,String> params = new HashMap<String,String>();
        for(String param : uriParams){
            int pos = param.indexOf(LOG_URI_PARAM_SEPRATOR);
            String key = param.substring(0, pos);
            String value = param.substring(pos + 1);
            params.put(key,value);
        }

        if( StringUtils.isBlank(params.get("action")) || StringUtils.isBlank(params.get("uid"))
                || StringUtils.isBlank(params.get("gid")) || StringUtils.isBlank(params.get("ts")) ){
            throw new Exception(" One gm params is null");
        }

        GMAction action = GMAction.getAction(params.get("action"));

        if(action == null){
            throw new Exception("Unknown action type " + params.get("action"));
        }

        long time = sdf.get().parse(attrs.get(1)).getTime();

        Put put;
        if(action != GMAction.LINK){
            byte[] rowKey = Bytes.add(Bytes.toBytes(action.getShortHand()),Bytes.toBytes(time),Bytes.toBytes(params.get("gid")));
            rowKey = Bytes.add(rowKey,Bytes.toBytes(ETLConstants.ROWKEY_SEP),Bytes.toBytes(params.get("uid")));

            put = new Put(rowKey);
            put.add(ucf,gidCol,time,Bytes.toBytes(params.get("gid")));

            String[] lang = params.get("l").split("-");
            String nation = lang.length==2? lang[1] : lang[0];

            putNotNull(put, ucf, langCol, time, params.get("l"));
            putNotNull(put,ucf,nationCol,time,nation.toLowerCase());
            putNotNull(put,ucf,tzCol,time,params.get("tz"));
            putNotNull(put,ucf,vipCol,time,params.get("v"));
            putNotNull(put,ucf,vaCol,time,params.get("va"));
            putNotNull(put,ucf,vpCol,time,params.get("vp"));
            putNotNull(put,ucf,vlCol,time,params.get("vl"));
            putNotNull(put,ucf,gameTypeCol,time,params.get("gt"));
            putNotNull(put,ucf,clCol,time,params.get("cl"));

            if(action == GMAction.HB){
                putNotNull(put,ucf,tagCol,time,params.get("tag"));
                putNotNull(put,ucf,titleCol,time,params.get("title"));
            }else if(action == GMAction.DOWN || action == GMAction.UP){
                putNotNull(put,ucf,countCol,time,params.get("c"));
                putNotNull(put,ucf,gsCol,time,params.get("gs"));
            }else if(action == GMAction.SHARE){
                putNotNull(put,ucf,fsCol,time,params.get("fs"));
                putNotNull(put,ucf,gsCol,time,params.get("gs"));
            }else if(action == GMAction.PAY){
                put.add(ucf,cashCol,time,Bytes.toBytes(params.get("cash")));
            }else if(action == GMAction.PLAY){
                putNotNull(put,ucf,recTypeCol,time,params.get("rt"));
                putNotNull(put,ucf,recIdxCol,time,params.get("idx"));
            }
        }else{
            byte[] rowKey = Bytes.toBytes(action.getShortHand());
            rowKey = Bytes.add(rowKey,Bytes.toBytes(params.get("uid")));

            put = new Put(rowKey);
            put.add(ucf, lidCol, time, Bytes.toBytes(params.get("lid")));
            LK_LOG.info(params.get("uid") + "\t" + params.get("lid") + "\t" + time);
        }
        return put;
    }

    @Override
    public void cleanup() throws Exception {
    }

    private void putNotNull(Put put,byte[] cf,byte[] col,long time ,String value){
        if(value != null){
            put.add(cf,col,time,Bytes.toBytes(value));
        }
    }

    private enum GMAction {

        HB("hb"){
            public String getShortHand(){
                return "hb";
            }
        },PLAY("play"){
            public String getShortHand(){
                return "pl";
            }
        },DOWN("down"){
            public String getShortHand(){
                return "do";
            }
        },UP("up"){
            public String getShortHand(){
                return "up";
            }
        },SHARE("share"){
            public String getShortHand(){
                return "sh";
            }
        },PAY("pay"){
            public String getShortHand(){
                return "pa";
            }
        },LINK("lk"){
            public String getShortHand(){
                return "lk";
            }
        };

        private GMAction(String type){
            this.type = type;
        }
        private final String type;
        public abstract  String getShortHand();
        public String getType(){
            return this.type;
        }

        public static GMAction getAction(String action){
            for(GMAction ga : GMAction.values()){
                if(ga.getType().equals(action)){
                    return ga;
                }
            }
            return null;
        }
    }

}
