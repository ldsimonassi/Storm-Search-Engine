package search.mocks;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class RandomFeedSpout implements IRichSpout {
	
	private static final long serialVersionUID = 1L;
	
	
	@SuppressWarnings("rawtypes")
	Map conf;
	TopologyContext context;
	SpoutOutputCollector collector;

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.conf = conf;
		this.context = context;
		this.collector = collector;
	}

	@Override
	public void close() {
	}

	@Override
	public void nextTuple() {
		long itemId= (long)(Math.random()*10000000l); // 10MM de items
		collector.emit(new Values(itemId));
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
		declarer.declare(new Fields("item_id"));
		
	}

	@Override
	public boolean isDistributed() {
		// TODO Auto-generated method stub
		return false;
	}

}
