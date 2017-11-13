package com.scistor.ETL;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public interface DataParseInterface {
    public abstract void init(Map<String,String> element, List<ArrayBlockingQueue<Map>> queue);
    public abstract void dataParse(File file) throws Exception;
    public abstract int report();
}
