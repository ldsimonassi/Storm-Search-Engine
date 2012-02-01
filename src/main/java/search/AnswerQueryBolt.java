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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

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
	
	@SuppressWarnings("unchecked")
	private void sendBack(String origin, String id, List<Item> finalResult){
		String to= "http://"+origin+":8082/?id="+id;
		
		System.out.println("Answering:"  + to);
		
		HttpPost post= new HttpPost(to);
		
		JSONArray list = new JSONArray();
		
		for (Item item : finalResult) {
			JSONObject obj= new JSONObject();
			obj.put("title", item.title);
			obj.put("id", item.id);
			obj.put("price", item.price);
			list.add(obj);
		}
		String json= JSONValue.toJSONString(list);
		try {
			StringEntity entity= new StringEntity(json);
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
