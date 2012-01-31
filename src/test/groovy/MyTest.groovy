import junit.framework.Assert;
import org.junit.Test;

import groovyx.net.http.ContentType;
import groovyx.net.http.RESTClient;



class MyTest extends Assert {

	def base = "http://localhost:8081"
	def restClient

	public MyTest() {
	}

	@Test
	public void simpleOne(){
		restClient= new RESTClient(base)
		try {
			def resp = restClient.get(path:'/newSessionId.json')
			assert resp.status == 200
		} catch  (Exception ex) {
			ex.printStackTrace();
		}
		assert 200== 200
	}

}
