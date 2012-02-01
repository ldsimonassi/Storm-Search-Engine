package search;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.msgpack.unpacker.Unpacker;

import search.model.Item;


public class SerializationUtils {

	public byte[] toByteArray(List<Item> list) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try{
	        Packer packer = msgpack.createPacker(out);
	        packer.write(list.size());

	        for (Item i : list) {
	        	packer.write(i.price);
	        	packer.write(i.title);
	        	packer.write(i.id);
			}
	        return out.toByteArray();
        } catch (Exception ex) {
        	ex.printStackTrace();
        	return null;
        } finally {
        	try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
	}
	
	MessagePack msgpack = new MessagePack();


	
	public Item itemFromByteArray(byte[] binary) {
		ByteArrayInputStream in = new ByteArrayInputStream(binary);
		try {
			Unpacker unpacker = msgpack.createUnpacker(in);
			Item i= new Item();
			i.price= unpacker.readDouble();
			i.title= unpacker.readString();
			i.id= unpacker.readLong();
			return i;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	
	
	public byte[] itemToByteArray(Item itm) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try{
	        Packer packer = msgpack.createPacker(out);
        	packer.write(itm);
        	packer.write(itm);
        	packer.write(itm);

	        return out.toByteArray();
        } catch (Exception ex) {
        	ex.printStackTrace();
        	return null;
        } finally {
        	try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
	}
	
	
	
	
	
	public List<Item> fromByteArray(byte[] binary) {
		ByteArrayInputStream in = new ByteArrayInputStream(binary);
		try {
			Unpacker unpacker = msgpack.createUnpacker(in);
			ArrayList<Item> list= new ArrayList<Item>();
			int size= unpacker.readInt();
			Item i= null;
			for (int j = 0; j < size; j++) {
				i= new Item();
				i.price= unpacker.readDouble();
				i.title= unpacker.readString();
				i.id= unpacker.readLong();
				list.add(i);
			}
			return list;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public Object toByteArray(Item i) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
