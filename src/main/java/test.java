import com.scistor.operator.FlumeClientOperator;
import com.scistor.operator.ZookeeperOperator;
import org.apache.zookeeper.data.Stat;

import java.util.Properties;

public class test {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("client.type", "default_failover");

// List of hosts (space-separated list of user-chosen host aliases)
        props.put("hosts", "h1 h2 h3");

// host/port pair for each host alias
        String host1 = "host1.example.org:41414";
        String host2 = "host2.example.org:41414";
        String host3 = "host3.example.org:41414";
        props.put("hosts.h1", host1);
        props.put("hosts.h2", host2);
        props.put("hosts.h3", host3);
        System.out.println(props.toString());

        //zookeeper test
        System.out.println(ZookeeperOperator.checkPath("/zookeeper/01"));
        ZookeeperOperator.registerSlaveInfo("/zookeeper/01","aaaaaaaaaaa");
        ZookeeperOperator.delete("/zookeeper/01","aaaaaaaaaaa");
//        ZookeeperOperator.registerSlaveInfo("/HS/1","aaaaaaaaaaa");
//        ZookeeperOperator.registerSlaveInfo("/HS/1/1","aaaaaaaaaaa");
//        ZookeeperOperator.registerSlaveInfo("/HS/1/1/produce","aaaaaaaaaaa");
        System.out.println(ZookeeperOperator.getZk().getChildren("/HS/1/1/produce",null));
        Stat stat = new Stat();
        System.out.println(ZookeeperOperator.getZk().getData("/HS/1/1/produce",null,stat));
        System.out.println(ZookeeperOperator.getZk().setData("/HS/1/1/produce",null,-1));
        System.out.println(stat.getAversion());
        ZookeeperOperator.delete("/HS/1/1/produce","aaaaaaaaaaa");
        System.out.println(ZookeeperOperator.checkPath("/HS/1/1/produce"));
        //flume client test
        for(int i=0; i<1; i++) {
            FlumeClientOperator.sendDataToFlume("this is a client test");
//            Thread.sleep(1000);
//            System.out.println("send" + i);
        }
        FlumeClientOperator.cleanUp();
        System.exit(0);
    }
}
