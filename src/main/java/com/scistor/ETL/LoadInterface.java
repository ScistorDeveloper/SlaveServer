package com.scistor.ETL;

public interface LoadInterface extends Runnable{
    public final String topicName="";
    public abstract void init();
}
