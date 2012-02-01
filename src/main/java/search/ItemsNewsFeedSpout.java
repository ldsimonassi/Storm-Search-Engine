package search;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.SingleClientConnManager;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ItemsNewsFeedSpout implements IRichSpout {
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("rawtypes")
	Map conf;
	TopologyContext context;
	SpoutOutputCollector collector;
	String feedPullHost;
	int maxPull;
	HttpClient httpclient;
	HttpGet httpget;
	
	/**
	 * Open a thread for each processed server.
	 */
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.conf= conf;
		this.context= context;
		this.collector= collector;
		this.feedPullHost= (String) conf.get("feed-pull-host");
		try{
			this.maxPull= Integer.parseInt((String)conf.get("max-pull"));
		} catch(Exception ex){
			this.maxPull= 1;
		}
		reconnect();
	}
	

	private void reconnect() {
		httpclient = new DefaultHttpClient(new SingleClientConnManager()); 
		httpget = new HttpGet("http://"+feedPullHost+"/?max="+maxPull); 
	}


	@Override
	public void close() {
	}
	
	int id=0;
	
	
	public static final int TIMEOUT= 1000;
	
	@Override
	public void nextTuple() {
		HttpResponse response;
		BufferedReader reader= null;
		try {
			response = httpclient.execute(httpget);
			HttpEntity entity = response.getEntity();
			reader= new BufferedReader(new InputStreamReader(entity.getContent()));
			String origin= reader.readLine();
			while(true) {
				String id= reader.readLine();
				if(id==null)
					break;
				String strId= reader.readLine();
				strId= strId.substring(1);
				int itemId= Integer.valueOf(strId);
				
				// I don't send the message id object, so I disable the ackers mechanism
				System.out.println("News feed emiting: ["+origin+"] ["+id+"] ["+itemId+"]");
				collector.emit(new Values(origin, id, itemId));
			} 
		} catch (Exception e) {
			e.printStackTrace();
			reconnect();
		} finally {
			if(reader!=null)
				try {
					reader.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
		}
		
	}


	@Override
	public void ack(Object msgId) {
	}

	@Override
	public void fail(Object msgId) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("origin", "requestId", "itemId"));
	}

	@Override
	public boolean isDistributed() {
		return true;
	}

}
