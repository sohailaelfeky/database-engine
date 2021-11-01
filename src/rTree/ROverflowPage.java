package rTree;

//import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
//import java.io.FileOutputStream;
import java.io.IOException;
//import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Properties;

public class ROverflowPage extends RRef implements Serializable {

	private static final long serialVersionUID = 1L;
	private int maxSize;
	private ArrayList<RRef> records;
	private ROverflowPage next;

	public ROverflowPage() {
		this.maxSize = getNodeSize();
		this.records = new ArrayList<RRef>();
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
	
	public ArrayList<RRef> getRecords() {
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
	
	public RRef getRecords(int i) {
		return this.records.get(i);
	}
	
	public ROverflowPage getNext() {
		return this.next;
	}
	
	public void setNext(ROverflowPage p) {
		this.next = p;
	}
	
	public String toString() {
		String output = "";
		for(int i = 0; i<this.records.size(); i++) {
			output += "pindex: " + this.records.get(i).getPage() + " ,rindex: " + this.records.get(i).getIndexInPage() + "\n";
		}
		return output;
	}
}