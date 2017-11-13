package com.scistor.process.thrift.server;

import com.scistor.operator.ZookeeperOperator;
import com.scistor.process.thrift.service.SlaveService;
import com.scistor.process.thrift.service.SlaveServiceImpl;
import org.apache.log4j.Logger;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Administrator on 2017/11/6.
 */
public class StartSlaveServer {

    private static final Logger LOG= Logger.getLogger(StartSlaveServer.class);
    private static String hostAddress;
    private static int PORT ;
    private static int SLAVES_THREAD_POOL_SIZE;
    private static String IP;
    private static String SLAVE_IP_DIR;
    public static void main(String[] args){
        // TODO Auto-generated method stub
        Properties props = new Properties();
        try {
            props.load(StartSlaveServer.class.getClassLoader().getResourceAsStream("config.properties"));
            PORT = Integer.parseInt(props.getProperty("thrift_slaveserver_port"));
            SLAVES_THREAD_POOL_SIZE = Integer.parseInt(props.getProperty("thread_pool_size"));
            SLAVE_IP_DIR = props.getProperty("thrift_slaveserver_dir");
            IP = InetAddress.getLocalHost().getHostAddress();
            start(args);
        } catch (Exception e) {
            String path = SLAVE_IP_DIR + IP + ":" + PORT;
            try {
                ZookeeperOperator.delete(path," ");
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    public static void start(String[] args) throws Exception {
        TProcessor processor = new SlaveService.Processor<SlaveService.Iface>(new SlaveServiceImpl());
        TNonblockingServerSocket serverSocket=new TNonblockingServerSocket(PORT);
        LOG.info("thrift transport init finished...");
        System.out.println("thrift transport init finished...");

        TThreadedSelectorServer.Args m_args=new TThreadedSelectorServer.Args(serverSocket);

        m_args.processor(processor);
        m_args.processorFactory(new TProcessorFactory(processor));
        m_args.protocolFactory(new TCompactProtocol.Factory());
        m_args.transportFactory(new TFramedTransport.Factory());
//        m_args.selectorThreads(RunningConfig.SLAVES_SELECTOR_THREADS);

        ExecutorService threads= Executors.newFixedThreadPool(SLAVES_THREAD_POOL_SIZE);
        m_args.executorService(threads);
        LOG.info("thrift nio channel selector  init finished...");
        System.out.println("thrift nio channel selector init finished...");
        TThreadedSelectorServer server=new TThreadedSelectorServer(m_args);
        LOG.info("ip:host send to zk");
        String path = SLAVE_IP_DIR + IP + ":" + PORT;
        ZookeeperOperator.registerSlaveInfo(path,path,"PERSISTENT");
        LOG.info("server starting...");
        System.out.println("server starting...");

        server.serve();
    }


}
