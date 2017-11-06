package com.scistor.process.thrift.service;

import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2017/11/6.
 */
public class SlaveServiceImpl implements SlaveService.Iface {
	@Override
	public String addSubTask(List<Map<String, String>> elements, String taskId, int slaveNo) throws TException {
		return null;
	}
}
