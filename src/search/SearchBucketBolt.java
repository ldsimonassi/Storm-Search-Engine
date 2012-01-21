package search;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import search.model.Item;
import search.model.ItemsDao;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class SearchBucketBolt implements IRichBolt {
	OutputCollector collector;
	Map stormConf;
	TopologyContext context;
	int currentShard;
	int totalShards;
	int base_id;
	SerializationUtils su;
	
	@Override
	public void prepare(Map stormConf, 
						TopologyContext context,
						OutputCollector collector) {
		this.stormConf= stormConf;
		this.context= context;
		this.collector= collector;
		currentShard = context.getThisTaskIndex();
		totalShards = context.getRawTopology().get_bolts().get(context.getThisComponentId()).get_common().get_parallelism_hint();
		System.out.println("SearchBucket Bolt created "+currentShard+" of "+totalShards);
		su= new SerializationUtils();	
		base_id= 10000*currentShard;
	}

	@Override
	public void execute(Tuple input) {
		// Get request routing information
		String origin= input.getString(0);
		String requestId= input.getString(1);
		String query= input.getString(2);
		
		// Execute query with local data scope
		List<Item> results= executeLocalQuery(query);
		
		//System.out.println("results for "+query+":"+results.size());
		
		// Send data to next step: Merger
		collector.emit(new Values(origin, requestId, query, su.toByteArray(results)));
	}
	
	
	
	private List<Item> executeLocalQuery(String query) {
		ArrayList<Item> list= new ArrayList<Item>();
		int size= (int)(Math.random()*10);
		for (int i = 0; i < size; i++) {
			list.add(new Item(base_id++, ItemsDao.getRandomTitle(), Math.random()*1000));	
		}
		return list;
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("origin", "requestId", "query", "shardMatches"));
	}
}
