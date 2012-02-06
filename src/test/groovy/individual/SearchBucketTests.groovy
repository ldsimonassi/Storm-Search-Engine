package individual

import org.junit.Test
import search.model.Item
import search.model.ItemsShard
import org.junit.Assert

public class SearchBucketTest extends Assert {
	@Test
	public void searchShard() {
		ItemsShard myShard= new ItemsShard(20);
		
		Item a= new Item(0, "nice dvd player with usb and card reader", 100);
		Item b= new Item(1, "new laptop computer with dvd and usb and manual", 100);
		Item c= new Item(2, "elegant cell phone with usb charger manual dvd included", 100);
		Item d= new Item(3, "new balck microwave includes cooking book and operation manual", 100);
		
		myShard.add(a);
		myShard.add(b);
		myShard.add(c);
		myShard.add(d);
		
		Set<Item> result= myShard.getItemsContainingWords("nice-dvd");
		assert result.contains(a);
		assert !result.contains(b);
		assert !result.contains(c);
		assert !result.contains(d);
		
		result= myShard.getItemsContainingWords("new-manual");
		assert result.contains(b);
		assert result.contains(d);
		assert !result.contains(a);
		assert !result.contains(c);

		result= myShard.getItemsContainingWords("nice-dvd-player-with-usb-and-card-reader");
		assert result.contains(a);
		assert !result.contains(b);
		assert !result.contains(c);
		assert !result.contains(d);
	}
}
