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
    private static String unZipHeaser = "E:\\01\\";
    private static String dstPath = "E:\\tmp\\upload.txt";
    private static ExecutorService fixedThreadPool;
    private static List<File> filelist = new ArrayList<File>();
    private boolean exit=false;
    private long dataSendCount = 0;
    private static ArrayBlockingQueue<Map> queue = new ArrayBlockingQueue<Map>(2000);
    public static void main(String[] args) throws Exception{

        //获取文件列表
        getFileList(unZipHeaser);
        System.out.println("file sum :" + filelist.size());
        if(filelist.size()==0){
            System.out.println("none files exist");
            System.exit(0);
        }
        List<Map<String, String>> elements = new ArrayList<Map<String, String>>();
        for(File f:filelist){
            final String fdir = f.getAbsolutePath();
            elements.add(new HashMap<String, String>(){
                {
                    put("mainclass","com.scistor.operator.DataParserOperator");
                    put("filedir",fdir);
                }
            });
            elements.add(new HashMap<String, String>(){
                {
                    put("mainclass","com.scistor.ETL.operator.HostFind");
                    put("Host","com.scistor.ETL.operator.HostFind");
                    put("task_type","producer");
                }
            });
            elements.add(new HashMap<String, String>(){
                {
                    put("mainclass","com.scistor.ETL.operator.HostFind");
                    put("Host","com.scistor.ETL.operator.HostFind");
                    put("task_type","producer");
                }
            });
            elements.add(new HashMap<String, String>(){
                {
                    put("mainclass","com.scistor.ETL.operator.HostFind");
                    put("Host","com.scistor.ETL.operator.HostFind");
                    put("task_type","producer");
                }
            });
            elements.add(new HashMap<String, String>(){
                {
                    put("mainclass","com.scistor.ETL.operator.HostFind");
                    put("Host","com.scistor.ETL.operator.HostFind");
                    put("task_type","consumer");
                }
            });
            SlaveServiceImpl sel = new SlaveServiceImpl();
            sel.addSubTask(elements,"1",1,true);
            break;
        }
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



    public static  void getFileList(String strPath) {
        File dir = new File(strPath);
        File[] files = dir.listFiles(); // 该文件目录下文件全部放入数组
        if (files != null) {
            for (int i = 0; i < files.length; i++) {
                String fileName = files[i].getName();
                if (files[i].isDirectory()) { // 判断是文件还是文件夹
                    getFileList(files[i].getAbsolutePath()); // 获取文件绝对路径
                } else if (fileName.endsWith("zip")) { // 判断文件名是否以.avi结尾
                    String strFileName = files[i].getAbsolutePath();
//                    System.out.println("---" + strFileName);
                    filelist.add(files[i]);
                } else {
                    continue;
                }
            }

        }
//        System.out.println(filelist.size());
    }



}
