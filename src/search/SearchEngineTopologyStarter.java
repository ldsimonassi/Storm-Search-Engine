package search;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class SearchEngineTopologyStarter {
	public static void main(String[] args) {
		TopologyBuilder builder= new TopologyBuilder();

		builder.setSpout("queries-generator", new QueriesSpout(), 6);
		builder.setBolt("queries-processor", new SearchBucketBolt(), 10).allGrouping("queries-generator");
		builder.setBolt("join-sort", new JoinSortBolt(), 10).fieldsGrouping("queries-processor", new Fields("origin", "requestId"));
		builder.setBolt("sender", new AnswerBolt(), 10).fieldsGrouping("join-sort", new Fields("origin"));

		Config conf= new Config();
		conf.put("server", "localhost:8081");
		conf.put("max", "100");
		
		if(args!=null && args.length > 0) {
			conf.setNumWorkers(20);
		
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
