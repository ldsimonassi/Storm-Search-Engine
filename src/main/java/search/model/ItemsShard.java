package search.model;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

/**
 *
 * This is a single threaded class intended to store a small number of documents.
 * 
 * @author ldsimonassi
 */
public class ItemsShard {
	HashMap<String, HashSet<Item>> index;
	HashMap<Item, Item> myItems;
	
	public ItemsShard(int initialCapacity) {
		// TODO Read Items from persistent storage.
		index = new HashMap<String, HashSet<Item>>(initialCapacity, 0.9f);
		myItems= new HashMap<Item, Item>(initialCapacity, 0.9f);
	}
	
	public synchronized void add(Item i) {
		if(myItems.containsKey(i))
			update(i);
		
		myItems.put(i, i);
		List<String> words= getItemWords(i);
		
		for (String word : words) {
			HashSet<Item> theSet= index.get(word);
			if(theSet==null) {
				theSet= new HashSet<Item>();
				index.put(word, theSet);
			}
			theSet.add(i);
		}
	}
	

	public void update(Item i) {
		//TODO Implement a more efficient, but more complex update operation (if necessary).
		// Update only if title changed
		if(!i.name.equals(myItems.get(i))){
			remove(i);
			add(i);
		}
	}
	
	public void remove(Item i) {
		if(myItems.containsKey(i)) {
			Item currentItem= myItems.get(i);
			List<String> words= getItemWords(currentItem);
			for (String word : words) {
				HashSet<Item> theSet= index.get(word);
				if(theSet==null) 
					throw new ConcurrentModificationException("Trying to remove an item which wasn't indexed, but its on de indexed list!");
				
				if(theSet.size()==1){
					index.remove(word);
				} else {
					theSet.remove(currentItem);
				}
			}
			myItems.remove(currentItem);
			
		}
	}

	
	private List<String> getItemWords(Item i) {
		ArrayList<String> ret = new ArrayList<String>();
		StringTokenizer strTok = new StringTokenizer(i.name, " ", false);

		while(strTok.hasMoreTokens()) {
			ret.add(strTok.nextToken());
		}
		return ret;
	}

	
	/**
	 * This method is highly concurrent
	 * @param word
	 * @return
	 */
	public Set<Item> getItemsContainingWord(String word) {
		Set<Item> items= index.get(word);
		//System.out.println("\tWord: ["+word +"] res:"+items.size());
		return items;
	}
	
	
	public Set<Item> getItemsContainingWords(String words){
		//System.out.println("Query: ["+words+"]");
	
		StringTokenizer strTok= new StringTokenizer(words, "-", false);
		HashSet<Item> result= null;
		boolean first= true;
		while(strTok.hasMoreTokens()){
			String word= strTok.nextToken();
			if(first){
				first= false;
				result= new HashSet<Item>(getItemsContainingWord(word));
			} else {
				Set<Item> newResults= getItemsContainingWord(word);
				
				for (Iterator<Item> iterator = result.iterator(); iterator.hasNext();) {
					Item item= iterator.next();
					if(!newResults.contains(item)){
						//System.out.println("\t\tremoving "+item);
						iterator.remove();
					}
				}
			}
		}
		return result;
	}

	public static void main(String[] args) {
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
		System.out.println(result.size());
		System.out.println(result);
		System.out.println("-----------------");
		assert result.contains(a);
		assert !result.contains(b);
		assert !result.contains(c);
		assert !result.contains(d);
		
		result= myShard.getItemsContainingWords("new-manual");
		System.out.println(result.size());
		System.out.println(result);
		System.out.println("-----------------");
		assert result.contains(b);
		assert result.contains(d);
		assert !result.contains(a);
		assert !result.contains(c);

		result= myShard.getItemsContainingWords("nice-dvd-player-with-usb-and-card-reader");
		System.out.println(result.size());
		System.out.println(result);
		System.out.println("-----------------");
		assert result.contains(a);
		assert !result.contains(b);
		assert !result.contains(c);
		assert !result.contains(d);
		
	}
	
}
