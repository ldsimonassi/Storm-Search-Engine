package smalltest;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Map;

import search.model.Item;
import search.model.ItemsDao;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class AlgoSpout implements IRichSpout {
	Map conf;
	TopologyContext context;
	SpoutOutputCollector collector;
	
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.conf = conf;
		this.context = context;
		this.collector = collector;
	}

	@Override
	public void close() {
		
	}

	int i= 0;
	
	@Override
	public void nextTuple() {
		i++;
		//collector.emit(new Values(i, "Dario"));
		
		int size= (int)(Math.random()*10);
		
		ArrayList<Item> list= new ArrayList<Item>();
		
		for (int i = 0; i < size; i++) {
			list.add(new Item(i, ItemsDao.getRandomTitle(), 100));
		}
		
		System.out.println("List has ["+list.size()+"] elements");
		
		try{
			ByteArrayOutputStream baos= new ByteArrayOutputStream();
			ObjectOutputStream oos= new ObjectOutputStream(baos);
			oos.writeObject(list);
			oos.close();
			baos.close();
			byte[] array= baos.toByteArray();
			System.out.println("Sending...:"+array.length);
			
			collector.emit(new Values(i, array));
		} catch (Exception ex){
			ex.printStackTrace();
		}
			
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "value"));
		
	}

	@Override
	public boolean isDistributed() {
		// TODO Auto-generated method stub
		return false;
	}

}
