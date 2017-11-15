/**
 * 主进程
 * 由unZipHeaser目录中读取数据,解析过滤后存放到dstPath中,
 * 参数定义单个文件长度,与数据生产线程数量
 */

import com.scistor.ETL.DataParseInterface;
import com.scistor.operator.DataParserOperator;
import com.scistor.operator.ZookeeperOperator;
import com.scistor.process.thrift.service.SlaveServiceImpl;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class Main {

    private static final Logger log = Logger.getLogger(Main.class);
    private static String unZipHeaser = "E:\\Project1\\HS\\02 数据资料\\170811\\";
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
                put("task_type","producer");
                put("taskId","dd70842d-a946-47b7-afeb-8ded58a73432");
            }
        });
        elements.add(new HashMap<String, String>(){
            {
                put("mainclass","com.scistor.operator.GeneratePasswordBook");
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
