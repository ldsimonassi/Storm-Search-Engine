package search;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import search.model.Item;
import search.model.ItemsDao;
import search.model.ItemsShard;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SearchBucketBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;

	OutputCollector collector;
	@SuppressWarnings("rawtypes")
	Map stormConf;
	TopologyContext context;
	int currentShard;
	int totalShards;
	int base_id;
	SerializationUtils su;
	ItemsShard shard;
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, 
						TopologyContext context,
						OutputCollector collector) {
		this.stormConf= stormConf;
		this.context= context;
		this.collector= collector;
		currentShard = context.getThisTaskIndex();
		String myId = context.getThisComponentId();
		totalShards = context.getRawTopology().get_bolts().get(myId).get_common().get_parallelism_hint();
		su = new SerializationUtils();
		shard = new ItemsShard(10000); 
	}
	
	private boolean isMine(int itemId) {
		int remain = itemId % totalShards; 
		return remain == currentShard; 
	}

	@Override
	public void execute(Tuple input) {
		if(input.getSourceComponent().equals("read-item-data")){
		
			String origin= input.getString(0);
			String requestId= input.getString(1);
			int itemId= input.getInteger(2);
			if(isMine(itemId)){
				byte[] ba = input.getBinary(3);
				if(ba==null) {
					System.out.println("Removing item id:"+itemId);
					shard.remove(itemId);
				} else {
					Item i= su.itemFromByteArray(ba);
					System.out.println("Updating item index: "+i);
					shard.update(i);
				}
			}
			return ;
		}

		// Get request routing information
		String origin= input.getString(0);
		String requestId= input.getString(1);
		String query= input.getString(2);
		
		// Execute query with local data scope
		List<Item> results= executeLocalQuery(query);
		
		// Send data to next step: Merger
		collector.emit(new Values(origin, requestId, query, su.toByteArray(results)));
	}

	private List<Item> executeLocalQuery(String query) {
		// TODO REMOVE
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
