package bPlusTree;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
//import java.io.FileOutputStream;
import java.io.IOException;
//import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Properties;

public class OverflowPage extends Ref implements Serializable {

	private static final long serialVersionUID = 1L;
	private int maxSize;
	private ArrayList<Ref> records;
	private OverflowPage next;

	public OverflowPage() {
		this.maxSize = getNodeSize();
		this.records = new ArrayList<Ref>();
		this.next = null;
	}

	private static int getNodeSize() {
		Properties properties = new Properties();
		FileInputStream inputStream;
		try {
			inputStream = new FileInputStream("config/DBApp.properties");
			properties.load(inputStream);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}

		String value = properties.getProperty("NodeSize");
		int n = Integer.parseInt(value);
		return n;
	}
	
	public int getMaxSize() {
		return this.maxSize;
	}
	
	public ArrayList<Ref> getRecords() {
		return this.records;
	}
	
	public int[] getMinPageNo() {
		int min = this.records.get(0).getPage();
		int r = this.records.get(0).getIndexInPage();
		return new int[] {min, r};
	}
	
	public int[] getMaxPageNo() {
		int last = this.records.size()-1;
		int max = this.records.get(last).getPage();
		int r = this.records.get(last).getIndexInPage();
		return new int[] {max, r};
	}
	
	public Ref getRecords(int i) {
		return this.records.get(i);
	}
	
	public OverflowPage getNext() {
		return this.next;
	}
	
	public void setNext(OverflowPage p) {
		this.next = p;
	}
	
	public String toString() {
		String output = "";
		for(int i = 0; i<this.records.size(); i++) {
			output += "pindex: " + this.records.get(i).getPage() + " ,rindex: " + this.records.get(i).getIndexInPage() + "\n";
		}
		return output;
	}
/*	
	public void savePage(Comparable key, int n) {
		try {
			File pa = new File("data/" + "/page_" + ".index.class");
			if(!pa.exists()) {
				pa.createNewFile();
			}
	        FileOutputStream fileOut = new FileOutputStream(pa);
	        ObjectOutputStream out = new ObjectOutputStream(fileOut);
	        out.writeObject(this);
	 		out.close();
	 		fileOut.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	      }
	}
	*/
}