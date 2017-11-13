/**
 * Flume集群运行在avro模式
 * 使用failover模式，如果一次send不成功，即重新选择一个节点重试。最大重试次数为3
 * 失败操作：1.汇报给主节点
 *           2.写入到本地文件
 */
package com.scistor.operator;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Properties;

public class FlumeClientOperator {
    private static RpcClient client = null;
    private static Properties props=null;
    static {
        try {
            // Setup properties for the failover
            InputStream in = FlumeClientOperator.class.getClassLoader().getResourceAsStream("flume.client.properties");
            props = new Properties();
            props.load(in);
            System.out.println(props.toString());
//            System.exit(1);
//            props.load(new FileInputStream("flume.client.properties"));
            // create the client with failover properties
            client = RpcClientFactory.getInstance(props);
            System.out.println("connect done!");
            in.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public static void sendDataToFlume(String data) throws Exception {
        // Create a Flume Event object that encapsulates the sample data
        Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));

        // Send the event
        try {
            if(client.isActive()) {
                client.append(event);
            }else{
                client.close();
                client = null;
                client = RpcClientFactory.getInstance(props);
                client.append(event);
            }
//            System.out.println("send event");
        } catch (EventDeliveryException e) {
            System.out.println("THIS IS FLUME CLIENT , WE ARE FACING AN EventDeliveryException");
            throw new Exception(e);
        }catch (Exception e){
            System.out.println("THIS IS FLUME CLIENT , WE ARE FACING AN Exception");
            throw new Exception(e);
        }
    }

    public static void cleanUp() {
        // Close the RPC connection
        client.close();
    }
}
