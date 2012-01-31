package search;

import java.io.InputStream;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.SingleClientConnManager;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class AnswerItemsFeedBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		client= new DefaultHttpClient(new SingleClientConnManager());
		this.su= new SerializationUtils();
	}

	SerializationUtils su;
	
	@Override
	public void execute(Tuple input) {
		String origin= input.getString(0);
		String requestId= input.getString(1);
		sendBack(origin, requestId);
	}
	
	
	HttpClient client;
	
	private void sendBack(String origin, String id){
		String to= "http://"+origin+":9092/?id="+id;
		HttpPost post= new HttpPost(to);
		try {
			StringEntity entity= new StringEntity("OK");
			post.setEntity(entity);
			

			HttpResponse response= client.execute(post);
			
			InputStream is= response.getEntity().getContent();
			
			is.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// No output, this is a terminal Bolt.
	}
}
