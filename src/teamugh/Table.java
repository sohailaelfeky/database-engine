package teamugh;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;

public class Table implements Serializable {

	private static final long serialVersionUID = 4234822112609915649L;
	private String tableName;
	private String clusteringKey;
	private String clusteringKeyType;
	private int clusteringKeyIndex;
	private ArrayList<String> colNames;
	private int noOfColumns;
	private int noOfPages;

	public Table(String strTableName, String strClusteringKeyColumn,Hashtable<String,String> htblColNameType) throws IOException, DBAppException {
		this.tableName = strTableName;
		this.clusteringKey = strClusteringKeyColumn;
		this.clusteringKeyType = this.clusteringKeyType(htblColNameType);
		this.colNames = new ArrayList<String>(htblColNameType.keySet());
		this.colNames.add("TouchDate");
		this.noOfColumns = colNames.size();
		this.clusteringKeyIndex = this.clusteringKeyIndex(htblColNameType);
		this.noOfPages = 0;
		CreationCheckers.addMeta(strTableName,strClusteringKeyColumn,htblColNameType);
	}
	public String getTableName() {
		return this.tableName;
	}
	public int getNoOfPages() {
		return this.noOfPages;
	}
	public void addPage() {
		this.noOfPages++;
	}
	public void removePage() {
		if(this.noOfPages!=0) {
			this.noOfPages--;
		}
	}
	public ArrayList<String> getColNames() {
		return this.colNames;
	}

	public int getNoOfColumns() {
		return this.noOfColumns;
	}

	public String getClusteringKey() {
		return this.clusteringKey;
	}
	public String getClusteringKeyType() {
		return this.clusteringKeyType;
	}
	public int getClusteringKeyIndex() {
		return this.clusteringKeyIndex;
	}
	public void setNoOfColumns(int n) {
		this.noOfColumns = n;
	}

	public void saveTable() {
		try {
			File pa = new File("data/" + this.getTableName() + ".ser");
			if(!pa.exists()) {
				pa.createNewFile();
			}
			FileOutputStream fileOut = new FileOutputStream(pa);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(this);
			out.close();
			fileOut.close();
			//System.out.println("Table saved successfully!");
		} 
		catch (IOException i) {
			System.out.println("Error saving table.");
		}
	}

	public static Table loadTable(String strTableName) {
		try {
			FileInputStream fileIn = new FileInputStream("data/" + strTableName + ".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Table t = (Table) in.readObject();
			in.close();
			fileIn.close();
			return t;
		}
		catch (IOException i) {
			i.printStackTrace();
		}
		catch (ClassNotFoundException c) {
			System.out.println("Table not found.");
			c.printStackTrace();
		}
		return null;
	}

	public void renamePages() {
		File directory = new File("data/" + this.getTableName());
		File[] pages = directory.listFiles();
		Arrays.sort(pages);
		int i = 1;
		for(File page : pages) {
			page.renameTo(new File("data/" + getTableName() + "/page" + i + ".ser"));
			i++;
		}
		pages = directory.listFiles();
		Arrays.sort(pages);
		i = 1;
		for(File page : pages) {
			page.renameTo(new File("data/" + getTableName() + "/page_" + i + ".ser"));
			i++;
		}
	}

	private String clusteringKeyType(Hashtable<String,String> htblColNameType) {
		Enumeration<String> e = htblColNameType.keys();
		while(e.hasMoreElements()) {
			String key = e.nextElement();
			String type = htblColNameType.get(key);
			if(key.equals(this.getClusteringKey())) {
				return type;
			}
		}
		return null;
	}

	private int clusteringKeyIndex(Hashtable<String,String> htblColNameType) {
		Enumeration<String> e = htblColNameType.keys();
		int i = 0;
		while(e.hasMoreElements()) {
			String key = e.nextElement();
			if(key.equals(this.getClusteringKey())) {
				return i;
			}
			i++;
		}
		return i;
	}
}