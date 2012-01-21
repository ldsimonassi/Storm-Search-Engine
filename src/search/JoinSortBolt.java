package search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import search.model.Item;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class JoinSortBolt implements IRichBolt {
	OutputCollector collector;
	Map stormConf;
	TopologyContext context;
	int totalShards;
	
	@Override
	public void prepare(Map stormConf, 
						TopologyContext context,
						OutputCollector collector) {
		this.stormConf= stormConf;
		this.context= context;
		this.collector= collector;
		this.su = new SerializationUtils();
		totalShards = context.getRawTopology().get_bolts().get("queries-processor").get_common().get_parallelism_hint();
	}

	
	HashMap<String, List<List<Item>>> inCourse= new HashMap<String, List<List<Item>>>();
	SerializationUtils su;
	
	@Override
	public void execute(Tuple input) {
		String origin= input.getString(0);
		String requestId= input.getString(1);
		String query= input.getString(2);
		byte[] binary= input.getBinary(3);
		List<Item> shardResults= su.fromByteArray(binary);
		String id= origin+"-"+requestId;
		List<List<Item>> finished= inCourse.get(id);
		
		if(finished==null){
			finished= new ArrayList<List<Item>>();
			inCourse.put(id, finished);
		}
		
		finished.add(shardResults);
		
		if(finished.size()>=totalShards){
			//System.out.println("Finishing query ["+id+"]");
			List<Item> finalResult= joinSortAndCut(finished);
			collector.emit(new Values(origin, requestId, su.toByteArray(finalResult)));
			inCourse.remove(id);
		}
	}

	private List<Item> joinSortAndCut(List<List<Item>> finished) {
		List<Item> finalList= new ArrayList<Item>();
		for (List<Item> list : finished) {
			finalList.addAll(list);
		}
		Collections.sort(finalList, new Comparator<Item>() {
			@Override
			public int compare(Item o1, Item o2) {
				if(o1.price>o2.price){
					return -1;
				} else if(o1.price<o2.price){
					return 1;
				} else {
					return 0;
				}
			}
		});
		finalList= new ArrayList<Item>(finalList.subList(0, 5));
		return finalList;
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("origin", "requestId", "finalResult"));
	}
}
