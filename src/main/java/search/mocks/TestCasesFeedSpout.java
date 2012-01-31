package search.mocks;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TestCasesFeedSpout implements IRichSpout {
	
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
		collector.emit(new Values(1));
		collector.emit(new Values(2));
		collector.emit(new Values(3));
		collector.emit(new Values(4));
		collector.emit(new Values(5));
	}

	@Override
	public void ack(Object msgId) {
	}

	@Override
	public void fail(Object msgId) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("item_id"));
	}

	@Override
	public boolean isDistributed() {
		return true;
	}

}