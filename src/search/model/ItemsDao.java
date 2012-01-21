package search.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ItemsDao {
	
	public static final long TOTAL_ITEMS= 100;
	public static final long BASE_ID= 0;
	
	int totalShards, currentShard;
	
	
	public static String[] possibleWords= new String[] {
		"dvd", "laptop", "usb", "home", "theater", "sound", 
		"notebook", "17", "15", "14", "ipod", "iphone", 
		"mac", "pava", "mate", "bombilla",
		"aire", "acondicionado", "departamento", 
		"casa", "cochera", "silla", "sillon", "mesa", "cable", 
		"cortina", "lavarropa", "lavavajilla", 
		"televisor", "led", "lcd", "ambientes", 
		"cuadro", "decoracion", "pintura", "jarron", "escultura", 
		"ventana", "vidrio", "aluminio", "pvc",
		"nokia", "1100", "blackberry", "curve", 
		"android", "samsung", "galaxy", "sII", "windows", "mobile",
		"aeromodelismo", "automodelismo", "bateria", 
		"motor", "control", "remoto", "alas", "avion", "pilas",
		"combustible", "autos", "peugeot", "206", "207", 
		"307", "308", "407", "408", "fiat", "uno", "palio", 
		"siena", "linea", "stilo", "idea", 
		"chevrolet", "corsa", "agile", "aveo", "vecra", 
		"astra", "cruze", "captiva", "volkswagen", "gol", "trend",
		"power", "fox", "suran", "bora", "vento", "passat", "cc", 
		"tiguan", "touareg", "ford", "fiesta", "ka", "kinetic", 
		"design", "focus", "mondeo", "ecosport", "kuga"
	};
	
	
	public static int MAX_WORDS= 10;
	
	
	public static String getRandomTitle(){
		int cantWords= (int)(Math.random()*(MAX_WORDS-1))+1;
		String res= "";
		for(int i= 0; i< cantWords; i++){
			int ind= (int)(Math.random()*(possibleWords.length-1));
			res+=possibleWords[ind]+" ";
		}
		return res;
	}
	
	
	public ItemsDao(int totalShards, int currentShard){
		this.totalShards= totalShards;
		this.currentShard= currentShard;
	}
	
	public Map<Long, Item> getItems(){
		int shardSize= (int)(TOTAL_ITEMS/totalShards);
		HashMap<Long, Item> items= new HashMap<Long, Item>(shardSize);
		
		for(long i=0; i<TOTAL_ITEMS; i++) {
			long hash= i%totalShards;
			if(hash==currentShard)
				items.put(new Long(i), new Item(i, getRandomTitle(), getRandomPrice()));
		}
		return items;
	}
	
	
	private double getRandomPrice() {
		return Math.random()*1000;
	}


	public static void main(String[] args) {
		
		
		for(int i= 0; i< 5;i ++){
			System.out.println("For "+i+" ----------------------");
			ItemsDao dao= new ItemsDao(5, i);
			Map<Long, Item> map= dao.getItems();
			
			for (Long itemId : map.keySet()) {
				System.out.println(map.get(itemId));
			}
		}

		
	}
}

