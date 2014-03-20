package com.elex.bigdata.zergling.etl;

import com.elex.bigdata.zergling.etl.hbase.HBasePutterV2;
import com.elex.bigdata.zergling.etl.hbase.HBaseResourceManager;
import com.elex.bigdata.zergling.etl.model.LogType;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Author: liqiang
 * Date: 14-3-17
 * Time: 下午2:01
 */
public class LogImport {

    public static void main(String args[]) throws Exception {
        if(args.length != 6){
            throw new Exception("Please specify the type,tableName,filePath , batch size and project id");
        }
        long startTime = System.currentTimeMillis();


        String type = args[0];
        String tableName = args[1];
        String filePath = args[2];
        String workersStr = args[3];
        String batchStr = args[4];
        String projectName = args[5]; //保留字段，多个项目的时候可以用上

        int batch = Integer.parseInt(batchStr);
        int workers = Integer.parseInt(workersStr);

        LogType logType = LogType.getLogType(type);
        if( logType == null){
            throw new Exception("Unknown log type");
        }
        Logger LOGGER = Logger.getLogger(type);

        ExecutorService service = new ThreadPoolExecutor(5,workers,60, TimeUnit.MILLISECONDS,new LinkedBlockingDeque<Runnable>());
        FileInputStream fis = null;
        BufferedReader reader = null;
        List<Future<String>> jobs = new ArrayList<Future<String>>();

        AtomicLong counter = new AtomicLong();
        Long totalCount = 0l;
        new HBaseResourceManager(10);
        System.out.println("Begin processing log " + filePath);
        try {
            fis = new FileInputStream(filePath);
            reader = new BufferedReader(new InputStreamReader(fis));
            String line;
            List<String> lines = new ArrayList<String>();

            while((line =  reader.readLine()) != null){
                lines.add(line);
                ++totalCount;
                if(lines.size() == batch){
                    System.out.print(".");
                    jobs.add(service.submit(new HBasePutterV2(logType,tableName,lines,counter)));
                    lines = new ArrayList<String>();
                }
            }
            if(lines.size() > 0){
                jobs.add(service.submit(new HBasePutterV2(logType,tableName,lines,counter)));
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        } finally {
            if(reader != null){
                reader.close();
            }
            if(fis != null){
                fis.close();
            }

        }
        for(Future<String> job : jobs){
            try {
                job.get(3,TimeUnit.MINUTES);
                System.out.print(".");
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage());
                e.printStackTrace();
            } catch (ExecutionException e) {
                LOGGER.error(e.getMessage());
                //TODO; 线程内部异常处理
                e.printStackTrace();
            } catch (TimeoutException e) {
                LOGGER.error(e.getMessage());
                //TODO: 超时异常
                e.printStackTrace();
            }
        }

        service.shutdownNow();
        System.out.println(".");
        System.out.println("Finished import log");
        System.out.println("Insert " + counter.get() + "/" + totalCount + " lines spend " + (System.currentTimeMillis() - startTime) + "ms ");
        LOGGER.info("Insert " + counter.get() + "/" + totalCount + " lines spend " + (System.currentTimeMillis() - startTime) + "ms ");
    }
}
