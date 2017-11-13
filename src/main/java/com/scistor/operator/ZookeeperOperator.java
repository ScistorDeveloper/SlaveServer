/**
 * 实现指定节点插入、删除、检查等操作
 */
package com.scistor.operator;

import com.alibaba.fastjson.JSON;
import com.scistor.utils.TaskResult;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.NOPLogger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ZookeeperOperator {
    private static String TASK_RESULT_PATH = "/HS/tasks";
    private static CountDownLatch connectedSignal = new CountDownLatch(1);
    private static final Logger LOG= Logger.getLogger(ZookeeperOperator.class);
    private static String hostList = null;
    private static ZooKeeper zkopt = null;
    private static int SESSION_TIMEOUT = 300;
    private static String IP;
    private static String PORT;
    static{
        try {
            //读取properties文件中的配置连接信息
            InputStream in = ZookeeperOperator.class.getClassLoader().getResourceAsStream("config.properties");
            Properties prop = new Properties();
            prop.load(in);

            //获取地址与端口信息
            hostList = prop.getProperty("zookeeper.urls");
            IP = InetAddress.getLocalHost().getHostAddress();
            PORT = prop.getProperty("thrift_slaveserver_port");
            connect(hostList);
            in.close();
        }catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    public static ZooKeeper getZk(){
        return zkopt;
    }
    /**
     * 连接zookeeper server
     */
    public static void connect(final String hosts) throws Exception {
        try {
            zkopt = new ZooKeeper(hosts, SESSION_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        LOG.info("ZooKeeper:" + hosts + " connected... ");
                        connectedSignal.countDown();
                    }
                    LOG.info("event.type == " + event.getType());
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean checkPath(String path){
        try{
            if(Objects.equals(zkopt, null)){
                LOG.info("ZOOKEEPER IS EXPIRED!");
                System.out.println("ZOOKEEPER IS EXPIRED!");
                connect(hostList);
            }
            if(zkopt.exists(path,null)==null) {
                LOG.error("path does not exist " + path);
                System.out.println("path does not exist!!");
                return false;
            }else{
                LOG.info("check path done ,path exist " + path);
                System.out.println("check path done ,path exist!!");
                return true;
            }

        }catch (Exception e ){
//            e.printStackTrace();
            LOG.error(e.toString());
        }
        return false;
    }
    public static void registerSlaveInfo( String path, String taskId, String mode) throws Exception {
        String result;
        if(Objects.equals(zkopt, null)){
            LOG.info("ZOOKEEPER IS EXPIRED!");
            System.out.println("ZOOKEEPER IS EXPIRED!");
            connect(hostList);
        }
        if(!Objects.equals(connectedSignal, null)){
            connectedSignal.await();
        }
        if(ZookeeperOperator.checkPath(path)){
            LOG.info(String.format("znode[%s] is exist",path));
            System.out.println("znode is exist , stat update is forbidden" + path);

            zkopt.setData(path, taskId.getBytes(), -1);
            System.out.println("delete old stats and update new stat" + path);
        }else{
            if(mode.equals("PERSISTENT")) {
                result = zkopt.create(path, taskId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                LOG.info("upload satus " + result);
            }else if(mode.equals("EPHEMERAL")){
                result = zkopt.create(path, taskId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                LOG.info("upload satus " + result);

            }
        }
        LOG.info("save slave info on zk,info=="+path+" "+taskId);
    }

    public static void delete(String path, String taskId) throws Exception {
        if(Objects.equals(zkopt, null)){
            connect(hostList);
        }
        if(ZookeeperOperator.checkPath(path)){
            //delete any version
            zkopt.delete(path,-1);
            LOG.info("path deleted " + path);
            System.out.println("path deleted"+path);
        }else{
            LOG.info(String.format("znode[%s] does not exists",path));

        }
    }


    public static void updateTaskResult(final CountDownLatch cdl, String taskId, String mainClass, TaskResult result) throws Exception{

        if(Objects.equals(zkopt, null)){
            LOG.info("ZOOKEEPER IS EXPIRED!");
            System.out.println("ZOOKEEPER IS EXPIRED!");
            connect(hostList);
        }
        if(!Objects.equals(connectedSignal, null)){
            connectedSignal.await();
        }

        String nodeName = IP + ":" + PORT;
        String zPath = TASK_RESULT_PATH + "/" + taskId + "/" + nodeName + "/" + mainClass;
        if(zkopt.exists(zPath, false) != null){
            zkopt.setData(zPath, JSON.toJSONString(result).getBytes(), -1);
            LOG.info(String.format(String.format("Update zk result, taskId:[%s], ZK_NODE_PATH:[%s], RESULT:[%s]", taskId, zPath, JSON.toJSONString(result))));
        }else{
            throw new IllegalArgumentException(String.format("taskId:[%s], ZK_NODE_PATH[%s], is not exists, check taskId, or new .......", taskId, zPath));
        }

    }
    public static void getData(String path, String taskId) throws Exception {
        if(Objects.equals(zkopt, null)){
            connect(hostList);
        }
        if(ZookeeperOperator.checkPath(path)){
            Stat stat = new Stat();
            zkopt.getData(path,null,stat);
            LOG.info("get Data " + stat.toString());
        }else{
            LOG.info(String.format("znode[%s] does not exists",path));

        }
    }
}
