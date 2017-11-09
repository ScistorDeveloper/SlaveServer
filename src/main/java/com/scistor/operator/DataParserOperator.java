package com.scistor.operator;

import com.scistor.ETL.DataParseInterface;
import com.scistor.utils.Map2String;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class DataParserOperator implements DataParseInterface, Runnable{
    private static final Log LOG = LogFactory.getLog(DataParserOperator.class);
    private File filedir;
    private List<ArrayBlockingQueue<Map>> queueList;
    public void DataParserOperator(){
    }
    @Override
    public void init(File filedir, List<ArrayBlockingQueue<Map>> queueList){
        this.filedir = filedir;
        this.queueList = queueList;
    }
    @Override
    public void run() {
        try{
            dataParse();
        }catch (Exception e){
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    @Override
    public void dataParse() throws Exception{
        int n = 0;
        InputStream in = null;
        ZipInputStream zin = null;
        FileOutputStream out = null;
        BufferedOutputStream writer = null;
        try {
            ZipFile zf = new ZipFile(filedir);
            in = new BufferedInputStream(new FileInputStream(filedir));
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

                        //放入数据队列，算子使用
                        for (int i=0;i<queueList.size();i++) {
                            queueList.get(i).put(data);
                            LOG.info("PUT DATA"  + data);
//                            System.out.println(data);
                        }
                        //直接发送到flume使用
                        String line = Map2String.transMapToString(data);
                        FlumeClientOperator.sendDataToFlume(line);
                        FlumeClientOperator.cleanUp();

                    }
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            in.close();
            zin.closeEntry();
        }
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
                        data.put(key, value);

                    }
                }catch (Exception e){
                    e.printStackTrace();
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
}
