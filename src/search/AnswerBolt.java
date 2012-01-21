package search;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
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

public class AnswerBolt implements IRichBolt {

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		client= new DefaultHttpClient();
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
		HttpPost post= new HttpPost("http://"+origin+":8082/?id="+id);
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

	private void saveToFile(String id, List<Item> finalResult) {
		File f= new File("/var/tmp/spool/"+id+".log");
		PrintStream pstr= null;
		try {
			pstr = new PrintStream(f);
		
			for (Item item : finalResult) {
				pstr.println(item);	
			}
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if(pstr!=null)
				pstr.close();
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
