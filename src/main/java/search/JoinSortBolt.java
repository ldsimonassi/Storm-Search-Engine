package search;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import search.model.Item;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class JoinSortBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;
	
	@SuppressWarnings("rawtypes")
	Map stormConf;
	TopologyContext context;
	int totalShards;
	OutputCollector collector;
	HashMap<String, Merger> inCourse= new HashMap<String, Merger>();
	SerializationUtils su;
	
	
	public static class Merger {
		ArrayList<Item> items;
		int size;
		int totalMerged;
		long start;
		String origin;
		String requestId;

		public List<Item> getResults(){
			return items;
		}

		public long getAge(){
			return System.currentTimeMillis()-start;
		}

		public String getId(){
			return getId(origin, requestId);
		}

		public static String getId(String origin, String requestId){
			return origin+"-"+requestId;
		}

		public Merger(String origin, String requestId, int size) {
			this.size = size;
			items = new ArrayList<Item>(size);
			totalMerged = 0;
			start= System.currentTimeMillis();
			this.origin = origin;
			this.requestId = requestId;
		}

		public int getTotalMerged(){
			return totalMerged;
		}

		public void merge(List<Item> newItems){
			totalMerged++;
			if(items.size()==0){
				int copy= newItems.size()<size?newItems.size():size;
				for(int i=0; i < copy;i++){
					items.add(i, newItems.get(i));
				}
			} else {
				ArrayList<Item> newList= new ArrayList<Item>();
				int iA = 0;
				int iB = 0;
				for(int i=0; i<size; i++){
					boolean overA= iA>items.size();
					boolean overB= iB>items.size();
					
					if(!overB && !overA){
						Item itmA= items.get(iA);
						Item itmB= newItems.get(iB);
						if(itmA.greaterThan(itmB)){
							iA++;
							newList.add(i, itmA);
						} else {
							iB++;
							newList.add(i, itmB);
						}
					} else if(overA && overB) {
						break;
					} else if(overA){
						Item itmB= newItems.get(iB);
						iB++;
						newList.add(i, itmB);
					} else { //overB
						Item itmA= items.get(iA);
						iA++;
						newList.add(i, itmA);	
					}
				}
				
				items= newList;
			}
		}
		
		@Override
		public String toString() {
			return items.toString();
		}

		public String getRequestId() {
			return requestId;
		}

		public Object getOrigin() {
			return origin;
		}
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.stormConf= stormConf;
		this.context= context;
		this.su = new SerializationUtils();
		this.collector = collector;
		totalShards = context.getRawTopology().get_bolts().get("queries-processor").get_common().get_parallelism_hint();
		TimerTask t= new TimerTask() {
			@Override
			public void run() {
				ArrayList<Merger> mergers;
				
				synchronized (inCourse) {
					mergers= new ArrayList<JoinSortBolt.Merger>(inCourse.values());
				}
				
				for (Merger merger : mergers) {
					if(merger.getAge()>1000)
						finish(merger);
				}
			}
		};
		
		Timer timer= new Timer();
		timer.scheduleAtFixedRate(t, 1000, 1000);
	}

	@Override
	public void execute(Tuple input) {
		String origin= input.getString(0);
		String requestId= input.getString(1);
		
		//String query= input.getString(2);
		byte[] binary= input.getBinary(3);
		List<Item> shardResults= su.fromByteArray(binary);

		String id = Merger.getId(origin, requestId);
		Merger merger= null;
		
		synchronized (inCourse) {
			merger= inCourse.get(id);	
		}
		
		
		if(merger==null){
			merger= new Merger(origin, requestId, 5);
			synchronized (inCourse) {
				inCourse.put(merger.getId(), merger);	
			}
		}

		merger.merge(shardResults);

		if(merger.getTotalMerged()>=totalShards){
			finish(merger);
			
		}		
	}
	
	
	protected void finish(Merger merger){
		collector.emit(new Values(merger.getOrigin(), merger.getRequestId(), su.toByteArray(merger.getResults())));
		synchronized (inCourse) {
			inCourse.remove(merger.getId());	
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("origin", "requestId", "results"));
	}
	
	public static void main(String[] args) {
		
		ArrayList<Item> a= new ArrayList<Item>();
		ArrayList<Item> b= new ArrayList<Item>();
		ArrayList<Item> c= new ArrayList<Item>();
		long id=0;
		for(int i=0; i<10;i++){
			a.add(new Item(id, "a", i));
			id++;
			b.add(new Item(id, "b", i+0.25));
			id++;
			c.add(new Item(id, "c", i+0.5));
			id++;
		}
		
		Merger m= new Merger("localhost", "44", 5);
		
		m.merge(a);
		System.out.println(m);
		m.merge(b);
		System.out.println(m);
		m.merge(c);
		System.out.println(m);
	}
}
