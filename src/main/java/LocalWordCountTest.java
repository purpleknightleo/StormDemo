import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.lee.demo.storm.bolt.WordCounter;
import com.lee.demo.storm.bolt.WordNormalizer;
import com.lee.demo.storm.spout.ShutdownSignal;
import com.lee.demo.storm.spout.WordReader;

/**
 * �ִ�ͳ�Ƶļ򵥱�������ʾ��
 * 
 * @author hzlifan
 *
 */
public class LocalWordCountTest {

	/**
	 * 2��spout��2��bolt
	 *
	 * @return
	 */
	private TopologyBuilder getBuilder() {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader());
		builder.setSpout("signaler", new ShutdownSignal());
		builder.setBolt("word-normalizer", new WordNormalizer(), 3)
				.shuffleGrouping("word-reader"); // ���ò��жȣ�����ɷ���������streamId=reader
		builder.setBolt("word-counter", new WordCounter(), 2)
				.fieldsGrouping("word-normalizer", new Fields("word"))
				.allGrouping("signaler", "signals"); // ���ò��жȣ�����field���ƶ����ɷ��������ͬһ������ʼ�ջ��ɷ���ͬһ��WordCounter�У�ͬʱ������һ��ShutdownSignalԴ��
		return builder;
	}

	private Config getConfig() {
		Config conf = new Config();
		conf.put("filePath", this.getClass().getResource("input/text.txt").getFile()); // �����ļ���
		conf.setDebug(true);
		return conf;
	}

	public void test() throws Exception {
		LocalCluster cluster = new LocalCluster();
		String topology = "Simple Word Count Topology Demo";
		cluster.submitTopology(topology, getConfig(), getBuilder()
				.createTopology());
		Thread.sleep(2000);
		cluster.killTopology(topology);
		cluster.shutdown();
	}

}
