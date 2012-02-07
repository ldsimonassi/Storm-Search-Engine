package simplesearch;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class SimpleSearchEngineTopologyStarter {
	public static StormTopology createTopology() {
		TopologyBuilder builder= new TopologyBuilder();

		builder.setSpout("queries-spout", new QueriesSpout(), 1);
		builder.setBolt("queries-processor", new SearchBolt(), 1).allGrouping("queries-spout");
		builder.setBolt("answer-query", new AnswerQueryBolt(), 2).allGrouping("queries-processor");
		return builder.createTopology();
	}

	public static Config createConf(String queriesPullHost, String feedPullHost, String itemsApiHost, int maxPull) {
		// Custom configuration
		Config conf= new Config();
		conf.put("queries-pull-host", queriesPullHost);
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
