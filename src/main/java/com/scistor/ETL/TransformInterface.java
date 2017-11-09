package com.scistor.ETL;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public interface TransformInterface {
    public abstract void init(Map<String,String> config, ArrayBlockingQueue<Map> queue);
    public abstract void validate();//参数校验
    public abstract void process();//编写业务处理逻辑
    public abstract void merge();//编写聚合逻辑
    public abstract void close();//关闭所用的资源

}
