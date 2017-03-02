package com.lee.demo.storm.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 将输入的数值乘以2并返回
 * 
 * @author hzlifan
 *
 */
public class MultiplierBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = -3259281576535100238L;

	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Double num = Double.valueOf(input.getString(1));
		collector.emit(new Values(input.getValue(0), num * 2)); // 将rpcId和结果返回
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "result")); // rpcId和结果
	}

}
