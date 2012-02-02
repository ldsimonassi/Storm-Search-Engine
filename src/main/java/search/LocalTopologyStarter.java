package search;

import java.util.Enumeration;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;

public class LocalTopologyStarter {
	
    public static void main(String[] args) {
    	Logger.getRootLogger().setLevel(Level.ERROR);
    	Logger.getLogger("org.apache.http.wire").setLevel(Level.ERROR);
    	//java.util.logging.Logger.getLogger("httpclient").setLevel(java.util.logging.Level.INFO);
		LocalCluster cluster = new LocalCluster();
		StormTopology topology = SearchEngineTopologyStarter.createTopology();
		Config conf = SearchEngineTopologyStarter.createConf("127.0.0.1:8081", "127.0.0.1:9091", "127.0.0.1:8888", 10);
		conf.setDebug(false);
		cluster.submitTopology("TestTopology", conf, topology);
	}
}
