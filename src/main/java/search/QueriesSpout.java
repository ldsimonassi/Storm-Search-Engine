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

public class QueriesSpout implements IRichSpout {
	private static final long serialVersionUID = 1L;
	
	public static final int TIMEOUT= 1000;
	
	@SuppressWarnings("rawtypes")
	Map conf;
	TopologyContext context;
	SpoutOutputCollector collector;
	String queriesPullHost;
	int maxPull;
	HttpClient httpclient;
	HttpGet httpget;
	int id=0;

	/**
	 * Open a thread for each processed server.
	 */
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.conf= conf;
		this.context= context;
		this.collector= collector;
		this.queriesPullHost= (String) conf.get("queries-pull-host");
		try{
			this.maxPull= Integer.parseInt((String)conf.get("max-pull"));
		} catch(Exception ex){
			this.maxPull= 1;
		}
		reconnect();
	}

	private void reconnect() {
		httpclient = new DefaultHttpClient(new SingleClientConnManager()); 
		httpget = new HttpGet("http://"+queriesPullHost+"/?max="+maxPull); 
	}

	@Override
	public void close() {
	}
	
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
				else {
					query = query.substring(1);
					// I don't send the message id object, so I disable the ackers mechanism
					collector.emit(new Values(origin, id, query));
				}
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
