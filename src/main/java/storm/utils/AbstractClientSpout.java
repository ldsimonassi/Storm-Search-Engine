package storm.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.SingleClientConnManager;
import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public abstract class AbstractClientSpout implements IRichSpout {
	private static final long serialVersionUID = 1L;
	public static final int TIMEOUT= 1000;
	transient Logger log;
	transient HttpClient httpclient;
	
	@SuppressWarnings("rawtypes")
	Map conf;
	TopologyContext context;
	SpoutOutputCollector collector;
	
	HttpGet httpget;
	int id=0;

	/**
	 * Open a thread for each processed server.
	 */
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		log = Logger.getLogger(this.getClass());
		this.conf= conf;
		this.context= context;
		this.collector= collector;
		reconnect();
	}

	protected abstract String getPullHost();
	protected abstract int getMaxPull();
	
	private void reconnect() {
		httpclient = new DefaultHttpClient(new SingleClientConnManager()); 
		httpget = new HttpGet("http://"+getPullHost()+"/?max="+getMaxPull()); 
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
				String requestContent= reader.readLine();
				if(id==null || requestContent==null)
					break;
				else {
					requestContent = requestContent.substring(1);
					// I don't send the message id object, so I disable the ackers mechanism
					collector.emit(new Values(origin, id, requestContent));
				}
			} 
		} catch (Exception e) {
			log.error(e);
			reconnect();
		} finally {
			if(reader!=null)
				try {
					reader.close();
				} catch (Exception e) {
					log.error(e);
				}
		}
	}


	@Override
	public void ack(Object msgId) {
		// Not used in this example, message delivery not warranted
	}

	@Override
	public void fail(Object msgId) {
		// Not used in this example, message delivery not warranted
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("origin", "requestId", "requestContent"));
	}

	@Override
	public boolean isDistributed() {
		return true;
	}

}
