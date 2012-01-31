package search;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.SingleClientConnManager;

import search.model.Item;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class AnswerQueryBolt implements IRichBolt {
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
		List<Item> finalResult= su.fromByteArray(input.getBinary(2));
		sendBack(origin, requestId, finalResult);
	}
	
	
	HttpClient client;
	
	private void sendBack(String origin, String id, List<Item> finalResult){
		String to= "http://"+origin+":8082/?id="+id;
		System.out.println("Answering to:["+to+"]");
		HttpPost post= new HttpPost(to);
		StringBuffer strBuff= new StringBuffer();
		for (Item item : finalResult) {
			strBuff.append(item.id);
			strBuff.append("\t");
			strBuff.append(item.name);
			strBuff.append("\t");
			strBuff.append(item.price);
			strBuff.append("\n");
		}
		
		try {
			StringEntity entity= new StringEntity(strBuff.toString());
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
