import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;

import com.lee.demo.storm.bolt.MultiplierBolt;

/**
 * 本地RPC调用
 * 
 * @author hzlifan
 *
 */
public class LocalRPCTest {

	private LinearDRPCTopologyBuilder getBuilder() {
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(
				"multiply two");
		builder.addBolt(new MultiplierBolt());
		return builder;
	}

	private Config getConfig() {
		Config conf = new Config();
		conf.setDebug(true);
		return conf;
	}

	public void test() {
		LocalDRPC drpc = new LocalDRPC();

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Local DRPC Topology", getConfig(), getBuilder()
				.createLocalTopology(drpc));
		String ret = drpc.execute("multiply two", Double.toString(7d));
		System.out.println("ret after multiplied by 2 = " + ret);
		cluster.shutdown();
	}

}
