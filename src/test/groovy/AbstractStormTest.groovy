import groovyx.net.http.ContentType;
import org.junit.Before
import org.junit.After
import org.junit.Assert
import groovyx.net.http.RESTClient

public abstract class AbstractStormTest extends Assert {
    def itemsApiClient
    def searchEngineApiClient
    def newsFeedApiClient

	@Before
    public void startLocalCluster() {
		//TODO setUp the storm environment
	}
	
	@After
	public void stopLocalCluster() {
		//TODO shutDown
	}

	@Before
    public void startRestClients() {
        itemsApiClient        = new RESTClient('http://127.0.0.1:8888')
        searchEngineApiClient = new RESTClient('http://127.0.0.1:8080')
        newsFeedApiClient     = new RESTClient('http://127.0.0.1:9090')
		clearItemsApi()
    }
	
	public void clearItemsApi() {
		def resp= itemsApiClient.delete(path : "/")
		assertEquals(resp.status, 200)
	}


	/**
	 *  Testing Utilities...
	 **/
	public void addItem(int id, String title, int price) {
		def document = "/${id}.json"
		def toSend = [:]
		toSend['id'] = id
		toSend['title'] = title
		toSend['price'] = price

        def resp= itemsApiClient.post(path : document,
                                      body: toSend,
                                      requestContentType: ContentType.JSON)
        assertEquals(resp.status, 200)
	}

	public void removeItem(int id) {
		def document = "/${id}.json"
        def resp= itemsApiClient.delete(path : document)
        assertEquals(resp.status, 200)
	}


	public Object readItem(int id) {
		def document = "/${id}.json"
		def resp = itemsApiClient.get(path:document)
		assertEquals(200, resp.status)
		assertEquals("${id}", "${resp.data.id}")

		return resp.data
	}

	public Object searchApi(String query) {
		def document = "/${query}.json"
		def resp = searchApiClient.get(path:document)

		assertEquals(200, resp.status)
		
		return resp.data
	}


	public void postNew(int id) {
		def document = "/${id}"
		def resp = newsFeedApiClient.get(path:document)

		assertEquals(200, resp.status)
	}
}
