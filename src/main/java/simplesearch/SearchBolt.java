package simplesearch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import search.model.Item;
import search.model.ItemsContainer;
import search.utils.SerializationUtils;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SearchBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;

	Logger log;
	OutputCollector collector;
	@SuppressWarnings("rawtypes")
	Map stormConf;
	TopologyContext context;
	int currentShard;
	int totalShards;
	int base_id;
	SerializationUtils su;
	ItemsContainer shard;
	
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, 
						TopologyContext context,
						OutputCollector collector) {
		log = Logger.getLogger(this.getClass());
		this.stormConf= stormConf;
		this.context= context;
		this.collector= collector;
		su = new SerializationUtils();
		shard = new ItemsContainer(10000); 
	}
	
	private boolean isMine(int itemId) {
		int remain = itemId % totalShards; 
		return remain == currentShard; 
	}

	@Override
	public void execute(Tuple input) {
		if(input.getSourceComponent().equals("read-item-data")){
			//String origin= input.getString(0);
			//String requestId= input.getString(1);
			int itemId= input.getInteger(2);
			if(isMine(itemId)){
				log.debug("Mine! "+currentShard+"/"+totalShards);
				byte[] ba = input.getBinary(3);
				if(ba==null) {
					log.debug("Removing item id:"+itemId);
					shard.remove(itemId);
				} else {
					Item i= su.itemFromByteArray(ba);
					log.debug("Updating item index: "+i);
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
		List<Item> results= executeLocalQuery(query, 5);
		log.debug("Searching ["+ query +"] in shard "+currentShard +" "+results.size()+" results found");
		// Send data to next step: Merger
		collector.emit(new Values(origin, requestId, query, su.toByteArray(results)));
	}

	private List<Item> executeLocalQuery(String query, int quantity) {
		List<Item> items= new ArrayList<Item>(shard.getItemsContainingWords(query));
		
		Collections.sort(items, new Comparator<Item>() {
			@Override
			public int compare(Item o1, Item o2) {
				double diff= o1.price-o2.price;
				if(diff>0)
					return 1;
				else
					return -1;
			}
		});
		
		if(items.size()>quantity)
			items = items.subList(0, quantity-1);
		return items;
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("origin", "requestId", "query", "shardMatches"));
	}
	
	
	
	
}
