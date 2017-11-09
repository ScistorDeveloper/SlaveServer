package com.scistor.ETL;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public interface DataParseInterface {
    public abstract void init(File filedir, List<ArrayBlockingQueue<Map>> queue);
    public abstract void dataParse() throws Exception;
    public abstract int report();
}
