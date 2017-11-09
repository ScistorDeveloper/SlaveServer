package com.scistor.ETL.operator;

import com.scistor.ETL.TransformInterface;
import com.scistor.operator.ZookeeperOperator;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public class HostFind implements TransformInterface {
    private String host;
    private ArrayBlockingQueue<Map> queue;
    private boolean is_continue =true;
    private String PRODUCE_PATH ;
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
    public void process() {

        while(is_continue){
            try {
                if(queue.size()>0) {
                    System.out.println("i have some data " +queue.size());
                    queue.take();
                }else {

                    //检查数据解析线程是不已经完成
                    if(!ZookeeperOperator.checkPath(PRODUCE_PATH)){
                        System.out.println("数据生产结束，退出");
                        is_continue=false;
                    }else{
                        System.out.println(PRODUCE_PATH);
                        Thread.sleep(2000);
                    }
                }
            }catch (Exception e){

                e.printStackTrace();
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
    }

    @Override
    public void close() {
        is_continue=false;
    }
}
