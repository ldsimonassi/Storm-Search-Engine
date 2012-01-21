package search.model;

import java.io.Serializable;

import org.msgpack.annotation.Message;

@Message
public class Item implements Serializable {
	
	public Item() {
		
	}
	
	public Item(long id, String title, double price) {
		this.id= id;
		this.name= title;
		this.price= price;
	}
	
	public long id;
	public String name;
	public double price;
	
	
	@Override
	public String toString() {
		return "id:"+id+ " title: "+name+ " price:"+price;
	}
}
