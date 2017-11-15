package com.scistor.process;
import com.scistor.process.thrift.service.SlaveServiceImpl;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;

public class main1 {
    private static final Logger log = Logger.getLogger(Main.class);
    private static String unZipHeaser = "E:\\01\\";
    private static String dstPath = "E:\\tmp\\upload.txt";
    private static ExecutorService fixedThreadPool;
    private static List<File> filelist = new ArrayList<File>();
    private boolean exit=false;
    private long dataSendCount = 0;
    private static ArrayBlockingQueue<Map> queue = new ArrayBlockingQueue<Map>(2000);
    public static void main(String[] args) throws Exception{

        List<Map<String, String>> elements = new ArrayList<Map<String, String>>();

        elements.add(new HashMap<String, String>(){
            {
                put("mainclass","com.scistor.operator.DataParserOperator");
                put("filedir",unZipHeaser);
                put("taskId","dd70842d-a946-47b7-afeb-8ded58a73432");
            }
        });
        elements.add(new HashMap<String, String>(){
            {
                put("mainclass","com.scistor.operator.GeneratePasswordBook");
                put("Host","com.scistor.ETL.operator.HostFind");
                put("task_type","producer");
                put("taskId","dd70842d-a946-47b7-afeb-8ded58a73432");
            }
        });

        elements.add(new HashMap<String, String>(){
            {
                put("mainclass","com.scistor.ETL.operator.HostFind");
                put("Host","com.scistor.ETL.operator.HostFind");
                put("task_type","consumer");
                put("taskId","dd70842d-a946-47b7-afeb-8ded58a73432");
            }
        });
        SlaveServiceImpl sel = new SlaveServiceImpl();
        String result = " ";
        try {
            result = sel.addSubTask(elements, "7d6c6cff-7894-4a81-99f0-873ef0fc60ca", "172.16.1.153:8089", true);
        }catch (Exception e){
            System.out.println("this is Main");
            e.printStackTrace();
            System.out.println(e.toString());
        }
        System.out.println("TEST Main is done "  + result);
//        System.exit(0);
//        System.out.println(System.getProperty("com.scistor.operator.DataParserOperator"));
//        ClassLoader cl = new ClassLoader() {
//            @Override
//            public Class<?> loadClass(String name) throws ClassNotFoundException {
//                return super.loadClass(name);
//            }
//        };
//        System.out.println(cl.loadClass("org.apache.log4j.Logger"));
//        System.out.println(cl.loadClass("com.scistor.ETL.operator.HostFind"));
    }
}
