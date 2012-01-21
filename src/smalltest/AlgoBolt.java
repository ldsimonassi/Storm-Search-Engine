package smalltest;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Map;

import search.model.Item;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class AlgoBolt implements IRichBolt{

	Map stormConf;
	TopologyContext context;
	OutputCollector collector;
	int currentShard;
	int totalShards;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.stormConf= stormConf;
		this.context= context;
		System.out.println("Preparing bolt! ["+this+"]");
		currentShard = context.getThisTaskIndex();
		totalShards = context.getRawTopology().get_bolts().get(context.getThisComponentId()).get_common().get_parallelism_hint();
		
		System.out.println("Loading data for shard ["+currentShard+" of "+totalShards+"]...");
		this.collector= collector;
	}

	@Override
	public void execute(Tuple input) {
		System.out.println("======== Execute: ["+input.getInteger(0)+"] ==============");
		try {
			ObjectInputStream ois= new ObjectInputStream(new ByteArrayInputStream(input.getBinary(1)));
			ArrayList<Item> list= (ArrayList<Item>)ois.readObject();
			for (int i = 0; i < list.size(); i++) {
				System.out.print(list.get(i));
				System.out.print(", ");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
