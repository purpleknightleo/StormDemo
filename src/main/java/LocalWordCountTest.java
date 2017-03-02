import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.lee.demo.storm.bolt.WordCounter;
import com.lee.demo.storm.bolt.WordNormalizer;
import com.lee.demo.storm.spout.ShutdownSignal;
import com.lee.demo.storm.spout.WordReader;

/**
 * 分词统计的简单本地拓扑示例
 * 
 * @author hzlifan
 *
 */
public class LocalWordCountTest {

	/**
	 * 2个spout，2个bolt
	 *
	 * @return
	 */
	private TopologyBuilder getBuilder() {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader());
		builder.setSpout("signaler", new ShutdownSignal());
		builder.setBolt("word-normalizer", new WordNormalizer(), 3)
				.shuffleGrouping("word-reader"); // 设置并行度，随机派发任务，设置streamId=reader
		builder.setBolt("word-counter", new WordCounter(), 2)
				.fieldsGrouping("word-normalizer", new Fields("word"))
				.allGrouping("signaler", "signals"); // 设置并行度，根据field名称定向派发任务，因此同一个单词始终会派发到同一个WordCounter中，同时再设置一个ShutdownSignal源端
		return builder;
	}

	private Config getConfig() {
		Config conf = new Config();
		conf.put("filePath", this.getClass().getResource("input/text.txt").getFile()); // 单词文件名
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
