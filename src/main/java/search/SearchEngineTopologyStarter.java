package search;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class SearchEngineTopologyStarter {
	public static StormTopology createTopology() {
		TopologyBuilder builder= new TopologyBuilder();

		builder.setSpout("queries-spout", new QueriesSpout(), 1);

		builder.setSpout("items-news-feed-spout", new ItemsNewsFeedSpout(), 1);
		builder.setBolt("read-item-data", new ReadItemDataBolt(), 2).shuffleGrouping("items-news-feed-spout");
		builder.setBolt("answer-items-feed", new AnswerItemsFeedBolt()).fieldsGrouping("read-item-data", new Fields("origin"));
		
		builder.setBolt("queries-processor", new SearchBucketBolt(), 10).allGrouping("queries-spout").allGrouping("read-item-data");
		
		builder.setBolt("join-sort", new JoinSortBolt(), 3).fieldsGrouping("queries-processor", new Fields("origin", "requestId"));
		builder.setBolt("answer-query", new AnswerQueryBolt(), 2).fieldsGrouping("join-sort", new Fields("origin"));

		return builder.createTopology();
	}

	public static Config createConf(String queriesPullHost, String feedPullHost, String itemsApiHost, int maxPull) {
		// Custom configuration
		Config conf= new Config();
		conf.put("queries-pull-host", queriesPullHost);
		conf.put("feed-pull-host", feedPullHost);
		conf.put("items-api-host", itemsApiHost);
		conf.put("max-pull", "100");
		// Disable ackers mechanismo for this topology which doesn't need to be safe.
		conf.put(Config.TOPOLOGY_ACKERS, 0);
		return conf;
	}
	
	public static void main(String[] args) {
		if(args.length < 5) {
			System.err.println("Incorrect parameters. Use: <name> <queries-pull-host> <feed-pull-host> <items-api-host> <max-pulling>");
			System.exit(-1);
		}
		
		System.out.println("Topology Name  ["+args[0]+"]");
        try {
        	Config conf= createConf(args[1], args[2], args[3], Integer.valueOf(args[4]));
    		conf.setNumWorkers(20);
			StormSubmitter.submitTopology(args[0], conf, createTopology());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
