package com.lee.demo.storm.spout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 读取文件每次获取一行
 * 
 * @author hzlifan
 *
 */
public class WordReader extends BaseRichSpout {

	private static final long serialVersionUID = -2846566789149759788L;

	private SpoutOutputCollector collector;

	private FileReader fileReader;

	private boolean completed = false;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		try {
			fileReader = new FileReader(conf.get("filePath").toString());
		} catch (Exception e) {
			System.out.println("fail to open word file");
			e.printStackTrace();
		}
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

		/** 以下读取文件逻辑只会运行一遍 */
		String line = null;
		BufferedReader br = new BufferedReader(fileReader);
		try {
			while ((line = br.readLine()) != null) {
				collector.emit(new Values(line), line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			completed = true;
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
