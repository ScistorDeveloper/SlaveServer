package com.scistor.utils;

import com.scistor.operator.ZookeeperOperator;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class ExceptionHandler implements Thread.UncaughtExceptionHandler {
    public void uncaughtException(Thread t, Throwable e) {
        System.out.printf("An exception has been captured\n");
        System.out.printf("Thread: %s\n", t.getId());
        System.out.printf("Exception: %s: %s\n",e.getClass().getName(), e.getMessage());
        System.out.printf("Stack Trace: \n");
        e.printStackTrace(System.out);
        System.out.printf("Thread status: %s\n", t.getState());
        String mess = e.getMessage();
        String task_id =mess.split("\\|\\|")[1];
        String mainclass =mess.split("\\|\\|")[0];
        TaskResult tr = new TaskResult(task_id,mainclass,true,e.toString());
        try {
            ZookeeperOperator.updateTaskResult(null,task_id,mainclass,tr);
        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }
}