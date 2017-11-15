package com.scistor.operator;

import com.scistor.ETL.DataParseInterface;
import com.scistor.utils.ExceptionHandler;
import com.scistor.utils.Map2String;
import com.scistor.utils.TaskResult;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class DataParserOperator implements DataParseInterface, Runnable{
    private static final Log LOG = LogFactory.getLog(DataParserOperator.class);
    private String filedir;
    private List<ArrayBlockingQueue<Map>> queueList;
    private Map<String,String> element;
    private static List<File> filelist = new ArrayList<File>();
    private int parsedCount=0;
    private static int neglectedCount = 0;
    private StringBuilder errorMess;
    public void DataParserOperator(){
    }
    @Override
    public void init(Map<String,String> element, List<ArrayBlockingQueue<Map>> queueList){
        this.element = element;
        this.filedir = element.get("filedir");
        LOG.info("got filedir is " + filedir);
        this.queueList = queueList;
    }
    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler(new ExceptionHandler());
        List<File> filelist = new ArrayList<File>();
        String task_id = element.get("taskId");
        String mainclass = element.get("mainclass");
        filelist = getFileList(task_id,filedir);
        LOG.info("parse operation got " + filelist.size() + " files");
        for(File file : filelist) {
            try {
                dataParse(file);
//                int number0 = Integer.parseInt("TTT");
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error("Caught EXCEPTION during file parsing... " + file.getAbsolutePath() + e);
                TaskResult tr = new TaskResult(task_id,mainclass,true,e.toString());
                try {
                    ZookeeperOperator.updateTaskResult(null,task_id,mainclass,tr);
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
                throw new RuntimeException(e.toString());
            }
        }
        errorMess.append("data parse succ,totle parsed "+parsedCount + "line data, and neglected " + neglectedCount
                +" lines data!! data ");
        TaskResult tr = new TaskResult(task_id,mainclass,true,errorMess.toString());
        try {
            ZookeeperOperator.updateTaskResult(null,task_id,mainclass,tr);
        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }

    @Override
    public void dataParse(File file) throws Exception{
        int n = 0;
        InputStream in = null;
        ZipInputStream zin = null;
        FileOutputStream out = null;
        BufferedOutputStream writer = null;
        ZipFile zf = new ZipFile(file);
        in = new BufferedInputStream(new FileInputStream(file));
        zin = new ZipInputStream(in);
        ZipEntry ze;
        while ((ze = zin.getNextEntry()) != null) {
            if (ze.isDirectory()) {
            } else {
                long size = ze.getSize();
                if (size > 0) {
                    InputStream is = zf.getInputStream(ze);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is));
                    //解析数据
                    Map<String,String> data = getMap(bufferedReader);
                    //数据为空，无操作
                    if(data==null){
                        continue;
                    }
                    //放入数据队列，算子使用
                    for (int i=0;i<queueList.size();i++) {
                        queueList.get(i).put(data);
                        LOG.debug("PUT DATA"  + data);
//                            System.out.println(data);
                    }
                    //直接发送到flume使用
                    try {
                        String line = Map2String.transMapToString(data);
                        FlumeClientOperator.sendDataToFlume(line);
                        FlumeClientOperator.cleanUp();
                    }catch (Exception e){
                        LOG.error("解析数据发送到FLUME错误，数据为："+Map2String.transMapToString(data));
                        System.out.println("解析数据发送到FLUME错误，数据为："+Map2String.transMapToString(data));
                        errorMess.append("解析数据发送到FLUME错误，数据为："+Map2String.transMapToString(data));
                    }
                    parsedCount ++;
                    if(parsedCount%5000==0){
                        LOG.info(Thread.currentThread().getName() + " has been parsed " + parsedCount + " lines data!");
                    }

                }
            }
        }
        in.close();
        zin.closeEntry();
    }
    /**
     * 由文件中取出Host 与 数据
     * @param br
     * @return Map<String,String></String,String>
     * * @throws Exception
     */
    private static Map<String,String> getMap(BufferedReader br) throws Exception {
        String str = null;
        String url="";
        Map<String,String> data = new HashMap<String, String>();
        List<String> line = new ArrayList<String>();
        while((str = br.readLine()) != null)
        {
            String host="Host";
            if(str.indexOf(host)!=-1){
                if(str.split(":").length>=2) {
//                    System.out.println(str);
                    url = str.split(":")[1].trim();
                }
                line.add(str.trim());
            }else{
                line.add(str.trim());
            }
        }
        if(line.size()>0) {
            data.put("Host",url);
            str = StringUtils.join(line, "[[--]]");
            if (str.indexOf("GET") >= 0) {
                //GET data
                String json = line.get(line.size()-1).replace("[[:]]",":");
                data.put("action_type","GET");
                try {
                    data.put("url",line.get(0).split(" ")[1]);
                    JSONObject jsonObject = JSONObject.fromObject(json);
                    Iterator iterator = jsonObject.keys();
                    String key = null;
                    String value = null;
                    while (iterator.hasNext()) {

                        key = (String) iterator.next();
                        value = jsonObject.getString(key);
                        data.put(key.toLowerCase(), value);

                    }
                }catch (Exception e){
                    //数据乱码等问题
                    LOG.debug("数据乱码？没有返回JSON？ " + url);
                    neglectedCount ++;
                    return null;
                }
            } else {
                //POST DATA
                data.put("action_type","POST");
                for (String li : line){
                    if(li.indexOf("[[:]]")>=0 & li.split("\\[\\[:\\]\\]").length>1){
                        data.put(li.split("\\[\\[:\\]\\]")[0],li.split("\\[\\[:\\]\\]")[1]);
                    }else if(li.indexOf(":")>=0 & li.split(":").length>1){
                        data.put(li.split(":")[0],li.split(":")[1]);
                    }
                }

            }
            return data;
        }else{
            return null;
        }

    }
    @Override
    public int report() {
        return 0;
    }


    public static  List<File> getFileList(String taskid, String strPath) {
        File dir = new File(strPath);
        if(!dir.isDirectory()){
            throw new RuntimeException(DataParserOperator.class.getName() + "||" + taskid + "||" + "PATH is not LEGAL");
        }
        File[] files = dir.listFiles(); // 该文件目录下文件全部放入数组
        if (files != null) {
            for (int i = 0; i < files.length; i++) {
                String fileName = files[i].getName();
                if (files[i].isDirectory()) { // 判断是文件还是文件夹
                    getFileList(taskid,files[i].getAbsolutePath()); // 获取文件绝对路径
                } else if (fileName.endsWith("zip")) { // 判断文件名是否以.zip结尾
                    String strFileName = files[i].getAbsolutePath();
//                    System.out.println("---" + strFileName);
                    filelist.add(files[i]);
                } else {
                    continue;
                }
            }

        }
//        System.out.println(filelist.size());
        return filelist;
    }
}
