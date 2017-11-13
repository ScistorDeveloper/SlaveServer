package com.scistor.ETL.operator;

import com.scistor.ETL.TransformInterface;
import com.scistor.operator.ZookeeperOperator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;

public class HostFind implements TransformInterface{
    private String host;
    private ArrayBlockingQueue<Map> queue;
    private boolean is_continue =true;
    private String PRODUCE_PATH ;
    private static final Log LOG = LogFactory.getLog(HostFind.class);
    @Override
    public void init(Map<String, String> config, ArrayBlockingQueue<Map> queue) {
        host = config.get("Host");
        this.queue = queue;
        PRODUCE_PATH = config.get("PRODUCE_PATH");
    }

    @Override
    public void validate() {

    }

    @Override
    public void process() throws Exception{

        while(is_continue){
            try {
                if(queue.size()>0) {
                    LOG.info("i have some data " +queue.size());
                    System.out.println("i have some data " +queue.size());
                    queue.take();
                }else {

                    //检查数据解析线程是不已经完成
                    if(!ZookeeperOperator.checkPath(PRODUCE_PATH)){
                        LOG.info("数据生产结束，退出");
                        System.out.println("数据生产结束，退出");
                        is_continue=false;
                    }else{
                        System.out.println(PRODUCE_PATH);
                        Thread.sleep(100);
                    }
                }
            }catch (Exception e){
//                e.printStackTrace();
                throw new Exception(this.getClass().getName() + e);
            }

        }
    }

    @Override
    public void merge() {
        /**
         * 为保证正常停止，必须为阻塞型线程
         * 条件为kafka中没有数据即停止
         */
        System.out.println("you are calling merge :" + this.getClass().getName());
        throw new RuntimeException("consume exception test");
    }

    @Override
    public void close() {
        is_continue=false;
    }

}
