import org.junit.Test;

public class SearchTest extends AbstractStormTest {

	public void loadItems() {
	}

	@Test
	public void newsFeedTest() {
		def result = searchApi("new")
		assertEquals(result.size(), 0)

		addItem(1, "new dvd player", 100)
		addItem(2, "new digital camera", 80)
		addItem(3, "new laptop computer", 70)
		postNew(1)
		postNew(2)
		postNew(3)


		result = searchApi("drive")
		assertEquals(0, result.size())

		result = searchApi("new")
		assertEquals(3, result.size())

		removeItem(1)
		removeItem(2)
		removeItem(3)
		postNew(1)
		postNew(2)
		postNew(3)

		result = searchApi("new")
		assertEquals(0, result.size())
	}

	@Test
	public void searchMultiple() {

	}
	
	@Test
	public void searchSingle() {
	}


	@Test
	public void onLineAddItem() {

	}

	@Test
	public void onLineRemoveItem() {
	
	}
}
