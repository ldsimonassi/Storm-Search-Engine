package search;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class SearchEngineTopologyStarter {
	public static void main(String[] args) {
		TopologyBuilder builder= new TopologyBuilder();

		builder.setSpout("queries-spout", new QueriesSpout(), 1);
		builder.setBolt("queries-processor", new SearchBucketBolt(), 10).allGrouping("queries-spout");
		builder.setBolt("join-sort", new JoinSortBolt(), 3).fieldsGrouping("queries-processor", new Fields("origin", "requestId"));
		builder.setBolt("sender", new AnswerBolt(), 2).fieldsGrouping("join-sort", new Fields("origin"));

		Config conf= new Config();
		
		// Disable ackers mechanismo for this topology which doesn't need to be safe.
		conf.put(Config.TOPOLOGY_ACKERS, 0);
		
		// Custom configuration
		conf.put("server", "ec2-23-20-23-116.compute-1.amazonaws.com:8081");
		//conf.put("server", "NodeJS-1561516991.us-east-1.elb.amazonaws.com:8081");
		
		conf.put("max", "100");
		
		
		if(args!=null && args.length > 0) {
			conf.setNumWorkers(20);
			System.out.println("Topology Name  ["+args[0]+"]");
            try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			LocalCluster cluster= new LocalCluster();
			cluster.submitTopology("TheSearchEngine", conf, builder.createTopology());
		}
	}
}
