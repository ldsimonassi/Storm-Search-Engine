package search.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.msgpack.unpacker.Unpacker;

import search.model.Item;


public class SerializationUtils {
	MessagePack msgpack = new MessagePack();
	Logger log= Logger.getLogger(this.getClass());

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
        	log.error(ex);
        	return null;
        } finally {
        	try {
				out.close();
			} catch (IOException e) {
				log.error(e);
			}
        }
	}
	
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
			log.error(e);
			return null;
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				log.error(e);
			}
		}
	}
	
	public byte[] itemToByteArray(Item itm) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try{
	        Packer packer = msgpack.createPacker(out);
        	packer.write(itm.price);
        	packer.write(itm.title);
        	packer.write(itm.id);

	        return out.toByteArray();
        } catch (Exception ex) {
        	log.error(ex);
        	return null;
        } finally {
        	try {
				out.close();
			} catch (IOException e) {
				log.error(e);
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
			log.error(e);
			return null;
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				log.error(e);
			}
		}
	}
}
