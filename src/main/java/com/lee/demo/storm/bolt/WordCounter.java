package com.lee.demo.storm.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * ���ʼ�����
 * 
 * @author hzlifan
 *
 */
public class WordCounter implements IRichBolt {

	private static final long serialVersionUID = 5816313254244597767L;

	private OutputCollector collector;

	private Map<String, Integer> countMap = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		countMap = new HashMap<String, Integer>();
	}

	@Override
	public void execute(Tuple input) {
		// ����ShutdownSignal��ָ����Ϣ
		if ("signals".equals(input.getSourceStreamId())) {
			System.out.println("receive signal : "
					+ input.getStringByField("action"));
		} else {
			// ����WordNormalizer��������Ϣ
			String word = input.getString(0);
			Integer count = countMap.get(word);
			if (count == null) {
				countMap.put(word, 1);
			} else {
				countMap.put(word, ++count);
			}
		}
		
		// ����ack
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}

	/*
	 * Topology�ر�ʱ����
	 * 
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#cleanup()
	 */
	@Override
	public void cleanup() {
		for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
			System.out.println("word = " + entry.getKey() + ", count = "
					+ entry.getValue());
		}
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
