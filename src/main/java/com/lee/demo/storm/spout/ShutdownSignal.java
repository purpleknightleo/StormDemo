package com.lee.demo.storm.spout;

import java.util.Map;
import java.util.UUID;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 关闭信号
 * 
 * @author hzlifan
 *
 */
public class ShutdownSignal extends BaseRichSpout {

	private static final long serialVersionUID = -2372457055318814569L;

	private SpoutOutputCollector collector;

	private boolean completed = false;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		if (completed) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return;
		}

		try {
			Thread.sleep(1000);
			collector.emit("signals", new Values("shutdown"), UUID.randomUUID()
					.toString()); // 只发送一次信号，并带有唯一的messageId，否则无法收到bolt的ack
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			completed = true;
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("signals", new Fields("action"));
	}

	@Override
	public void ack(Object msgId) {
		System.out.println("receive ack");
	}

}
