package smalltest;

import java.util.HashMap;

import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class AlgoTopology {
	public static void main(String[] args) throws InterruptedException {
		TopologyBuilder builder= new TopologyBuilder();
		builder.setSpout("MainSpout", new AlgoSpout(), 2);
		builder.setBolt("MainBolt", new AlgoBolt(), 7).fieldsGrouping("MainSpout", new Fields("id"));
		builder.setBolt("MainBolt2", new AlgoBolt(), 3).fieldsGrouping("MainSpout", new Fields("id"));
		
		
		LocalCluster cluster= new LocalCluster();
		cluster.submitTopology("TheTopology", new HashMap(), builder.createTopology());
		
		Thread.sleep(10000);
		
		cluster.shutdown();
		
	}
}
