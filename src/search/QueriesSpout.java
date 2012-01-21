package search;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.SingleClientConnManager;


import search.model.ItemsDao;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class QueriesSpout implements IRichSpout {

	Map conf;
	TopologyContext context;
	SpoutOutputCollector collector;
	String server;
	int max;
	HttpClient httpclient;
	HttpGet httpget;
	
	/**
	 * Open a thread for each processed server.
	 */
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.conf= conf;
		this.context= context;
		this.collector= collector;
		this.server= (String) conf.get("server");
		try{
			this.max= Integer.parseInt((String)conf.get("max"));
		} catch(Exception ex){
			this.max= 1;
		}
		reconnect();
	}
	

	private void reconnect() {
		httpclient = new DefaultHttpClient(new SingleClientConnManager()); 
		httpget = new HttpGet("http://"+server+"/?max="+max); 
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
				String query= reader.readLine();
				if(id==null || query==null)
					break;
				else
					collector.emit(new Values(origin, id, query));
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

	private String getRandomSearchQuery() {
		int idx= (int)(ItemsDao.possibleWords.length*Math.random());
		return ItemsDao.possibleWords[idx];
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
		declarer.declare(new Fields("origin", "requestId", "query"));
	}

	@Override
	public boolean isDistributed() {
		return true;
	}

}
