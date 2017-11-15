package com.scistor.process;
import com.scistor.operator.ZookeeperOperator;

public class zkTest {
    public static void main(String[] args) {
        if(ZookeeperOperator.checkPath("/HS/slave/172.16.1.153:8089/7d6c6cff-7894-4a81-99f0-873ef0fc60ca")){
        }

        System.out.println(ZookeeperOperator.checkPath("/HS"));
        System.out.println(ZookeeperOperator.checkPath("/HS/slave"));
        System.out.println(ZookeeperOperator.checkPath("/HS/slave/172.16.1.153:8089"));
        System.out.println(ZookeeperOperator.checkPath("/HS/slave/172.16.1.153:8089/7d6c6cff-7894-4a81-99f0-873ef0fc60ca"));

        try {
            ZookeeperOperator.registerSlaveInfo("/HS/slave/172.16.1.153:8089","sdf","PERSISTENT");
            ZookeeperOperator.registerSlaveInfo("/HS/slave/172.16.1.153:8089/7d6c6cff-7894-4a81-99f0-873ef0fc60ca","sdf","PERSISTENT");
            ZookeeperOperator.getZk().setData("/HS/slave/172.16.1.153:8089/test", "test".getBytes(), -1);
            ZookeeperOperator.registerSlaveInfo("/HS/slave/172.16.1.153:8089/7d6c6cff-7894-4a81-99f0-873ef0fc60ca","sdf","PERSISTENT");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
