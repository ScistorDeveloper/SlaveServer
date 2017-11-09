package com.scistor.process.thrift.service;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;

import com.scistor.ETL.TransformInterface;
import org.apache.log4j.Logger;

public class HandleNewTask implements Runnable{
	private HashMap<String, String> element;
	private static final Logger LOG = Logger.getLogger(HandleNewTask.class);
	public  ClassLoader classLoader=null;
	private ArrayBlockingQueue<Map> queue = null;
	private String PRODUCE_PATH;//必须是实例 变量

	public HandleNewTask(Map<String, String> element, ArrayBlockingQueue<Map> queue, String prodece_flag) {
		super();
		this.element = (HashMap<String, String>) element;
		this.queue = queue;
		this.PRODUCE_PATH = prodece_flag;

		Properties props = new Properties();
		try {
			props.load(SlaveServiceImpl.class.getClassLoader().getResourceAsStream("config.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		File compenents=new File(props.getProperty("jar_path"));
		System.out.println(compenents.getAbsolutePath());
		File[] files=compenents.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File arg0, String arg1) {
				return arg0.getName().endsWith(".jar");
			}
		});
		URL[] urls=new URL[files==null?0:files.length];
		for(int i=0;i<files.length;i++){
			try {
				urls[i]=files[i].toURI().toURL();
			} catch (MalformedURLException e) {
				LOG.info(e);
			}
		}
		if(urls==null||urls.length==0){
			classLoader=Thread.currentThread().getContextClassLoader();
		}else{
			classLoader=new URLClassLoader(urls,Thread.currentThread().getContextClassLoader());
		}
	}

	@Override
	public void run() {
		try {
			long startTime = 0;
			long endTime = 0;
			String mainclass = element.get("mainclass");
			String task_type = element.get("task_type");
			element.put("PRODUCE_PATH",PRODUCE_PATH);
			LOG.info("process " + mainclass);
			Class reflectClass = classLoader.loadClass(mainclass);
			Object reflectObject = reflectClass.newInstance();
			TransformInterface entry = (TransformInterface) reflectObject;
			entry.init(element, queue);
			if(task_type.equals("producer")) {
				entry.process();
			}else if(task_type.equals("consumer")){
				entry.merge();
			}
		}catch (Exception e){
			LOG.error(e.toString());
			e.printStackTrace();
		}

	}
}
