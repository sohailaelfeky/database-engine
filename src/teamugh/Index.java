package teamugh;

import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import bPlusTree.BPTree;
import bPlusTree.Ref;
import rTree.RRef;
import rTree.RTree;

public class Index {

	@SuppressWarnings("resource")
	public static boolean checkCOlExists(String strTableName, String strColName) {
		if(strColName.equals("TouchDate")) {
			return true;
		}
		try {
			String line = null;			
			BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));

			while ((line = br.readLine()) != null) {		
				String[] content = line.split(",");
				if(strTableName.equals(content[0]) && (strColName.equals(content[1]))) {
					return true;
				}
			}
			br.close();
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	@SuppressWarnings("resource")
	public static boolean hasIndex(String strTableName, String strColName) {
		if(strColName.equals("TouchDate")) {
			File file = new File("data/" + strTableName + ".TouchDate.index.class");
			if(file.exists()) {
				return true;
			}
			return false;
		}
		try {
			String line = null;			
			BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));

			while ((line = br.readLine()) != null) {		
				String[] content = line.split(",");
				if(strTableName.equals(content[0]) && (strColName.equals(content[1])) && content[4].equalsIgnoreCase("true")) {
					return true;
				}
			}
			br.close();
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	//@SuppressWarnings("resource")
	public static int findIndex(String strTableName, String strColName) throws DBAppException {
		Table t = Table.loadTable(strTableName);
		return t.getColNames().indexOf(strColName);
//		try {
//			String line = null;			
//			BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
//			int ind = 0;
//
//			while ((line = br.readLine()) != null) {		
//				String[] content = line.split(",");
//				if(strTableName.equals(content[0]) && (strColName.equals(content[1]))) {
//					return ind;
//				}
//				if(strTableName.equals(content[0]))
//					ind++;
//			}
//			br.close();
//		} 
//		catch (IOException e) {
//			System.out.println("metadata not found.");
//		}
//		return -1;
	}

	public static String columnType(String strTableName, String strColumnName) throws DBAppException {
		String type = null;
		String temp = null;
		if(strColumnName.equals("TouchDate"))
			return "Date";
		try {
			String line = null;			
			BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));

			while ((line = br.readLine()) != null) {		
				String[] content = line.split(",");
				if(strTableName.equals(content[0]) && (strColumnName.equals(content[1]))) {
					temp = content[2];
				}
			}
			br.close();
		}
		catch (IOException e) {
			System.out.println("metadata not found.");
			return type;
		}	
		switch(temp) { 
		case "java.lang.Integer":
			type = "Integer";
			break;
		case "java.lang.String":
			type = "String";
			break;
		case "java.lang.Double":
			type = "Double";
			break;
		case "java.awt.Polygon": 
			type = "Polygon";
			break;
		case "java.lang.Boolean":  
			type = "Boolean";
			break;
		case "java.util.Date" : 
			type = "Date";			
			break;
		default:
			throw new DBAppException();
		}	
		return type;
	}

	@SuppressWarnings({ "rawtypes", "unchecked", "resource" })
	public static void createBindex( String strTableName, String strColName) throws DBAppException, IOException, ClassNotFoundException {

		Properties properties = new Properties();
		FileInputStream inputStream = new FileInputStream("config/DBApp.properties");
		properties.load(inputStream);

		String value = properties.getProperty("NodeSize");
		int N = Integer.parseInt(value);
		int index = findIndex(strTableName, strColName);
		BPTree bt = null;	
		String type = columnType(strTableName, strColName);

		if(type.equals("Integer"))
			bt = new BPTree<Integer>(N);
		if(type.equals("String"))
			bt = new BPTree<String>(N);
		if(type.equals("Date"))
			bt = new BPTree<Date>(N);
		if(type.equals("Double"))
			bt = new BPTree<Double>(N);
		if(type.equals("Boolean"))
			bt = new BPTree<Boolean>(N);
		if(type.equals("Polygon"))
			throw new DBAppException("A B+ Tree index cannot be created for a column of type polygon.");

		FileInputStream fi = new FileInputStream("data/" + strTableName + ".ser");
		ObjectInputStream oi = new ObjectInputStream(fi);
		Table table = (Table) oi.readObject();

		Page p = new Page();
		int pages = table.getNoOfPages();

		if(pages > 0) {
			for(int i = 0; i < pages; i++) {	//loop each page
				p = Page.loadPage(strTableName, (i+1));	//load the page			
				for(int j = 0; j < p.size(); j++) { //loop each row in each page			
					Object[] temp = p.get(j);
					//insert chosen column into tree
					if(temp != null) {
						bt.insert((Comparable) temp[index], new Ref(i, j));
					}	
				}
			}
		}
		//serialize b+ tree
		ObjectOutputStream oos = new ObjectOutputStream(
				new FileOutputStream(new File("data/" + strTableName + "." + strColName + ".index.class")));
		oos.writeObject(bt);
		oos.close();

		//update meta
		ArrayList<String[]> lines = new ArrayList<String[]>();
		BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
		String line = "";
		while ((line = br.readLine()) != null) {
			String[] s = line.split(",");
			if(strTableName.equals(s[0]) && strColName.equals(s[1]))
				s[4] = "true"; 
			lines.add(s);
		}
		br.close();

		FileWriter w = new FileWriter("data/metadata.csv", false);
		for(int c=0; c<lines.size(); c++) {
			String[] s = lines.get(c);
			line = "";
			for(int d=0; d<s.length; d++) line += ("," + s[d]);
			w.write(line.substring(1) + "\n");
		}
		w.close();
	}
	@SuppressWarnings({ "rawtypes", "unchecked", "resource" })
	public static void createRindex( String strTableName, String strColName) throws DBAppException, IOException, ClassNotFoundException {
		Properties properties = new Properties();
		FileInputStream inputStream = new FileInputStream("config/DBApp.properties");
		properties.load(inputStream);

		String value = properties.getProperty("NodeSize");
		int N = Integer.parseInt(value);
		int index = findIndex(strTableName, strColName);	
		String type = columnType(strTableName, strColName);
		if(!type.equals("Polygon"))
			throw new DBAppException("A B+ Tree index cannot be created for a column with type polygon");
		RTree rt = new RTree(N);
		FileInputStream fi = new FileInputStream("data/" + strTableName + ".ser");
		ObjectInputStream oi = new ObjectInputStream(fi);
		Table table = (Table) oi.readObject();
		Page p = new Page();
		int pages = table.getNoOfPages();
		if(pages > 0) {
			for(int i = 0; i < pages; i++) {	//loop each page
				p = Page.loadPage(strTableName, (i+1));	//load the page			
				for(int j = 0; j < p.size(); j++) { //loop each row in each page			
					Object[] temp = p.get(j);
					//insert chosen column into tree
					if(temp != null) {
						rt.insert((Polygon) temp[index], new RRef(i, j));
					}	
				}
			}
		}
		//serialize r tree
		ObjectOutputStream oos = new ObjectOutputStream(
				new FileOutputStream(new File("data/" + strTableName 
						+ "." + strColName + ".index.class")));
		oos.writeObject(rt);
		oos.close();

		//update meta
		ArrayList<String[]> lines = new ArrayList<String[]>();
		BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
		String line = "";
		while ((line = br.readLine()) != null) {
			String[] s = line.split(",");
			if(strTableName.equals(s[0]) && strColName.equals(s[1]))
				s[4] = "true"; 
			lines.add(s);
		}
		br.close();

		FileWriter w = new FileWriter("data/metadata.csv", false);
		for(int c=0; c<lines.size(); c++) {
			String[] s = lines.get(c);
			line = "";
			for(int d=0; d<s.length; d++) line += ("," + s[d]);
			w.write(line.substring(1) + "\n");
		}
		w.close();
	}

	@SuppressWarnings("rawtypes")
	public static BPTree loadBTIndex(String strTableName, String strColName) {
		try {
			FileInputStream fileIn = new FileInputStream("data/" + strTableName + "." + strColName + ".index.class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			BPTree e = (BPTree) in.readObject();
			in.close();
			fileIn.close();
			return e;
		}
		catch (IOException | ClassNotFoundException i) {
			System.out.println("B+ Tree index for column \"" + strColName + "\" not found.");
		}
		return null;
	}

	@SuppressWarnings("rawtypes")
	public static RTree loadRTIndex(String strTableName, String strColName) {
		try {
			FileInputStream fileIn = new FileInputStream("data/" + strTableName + "." + strColName + ".index.class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			RTree e = (RTree) in.readObject();
			in.close();
			fileIn.close();
			return e;
		}
		catch (IOException | ClassNotFoundException i) {
			System.out.println("R Tree index for column \"" + strColName + "\" not found.");
		}
		return null;
	}

	@SuppressWarnings("rawtypes")
	public static void saveBPlusTree(BPTree bTree, String strTableName, String strColName) {
		try {
			File pa = new File("data/" + strTableName 
					+ "." + strColName + ".index.class");
			if(!pa.exists()) {
				pa.createNewFile();
			}
			FileOutputStream fileOut = new FileOutputStream(pa);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(bTree);
			out.close();
			fileOut.close();

			System.out.println("B+ Tree saved successfully!");
		} 
		catch (IOException i) {
			i.printStackTrace();
		}
	}

	@SuppressWarnings("rawtypes")
	public static void saveRTree(RTree rTree, String strTableName, String strColName) {
		try {
			File pa = new File("data/" + strTableName + "." + strColName + ".index.class");
			if(!pa.exists()) {
				pa.createNewFile();
			}
			FileOutputStream fileOut = new FileOutputStream(pa);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(rTree);
			out.close();
			fileOut.close();
			System.out.println("R Tree saved successfully!");
		} 
		catch (IOException i) {
			i.printStackTrace();
		}
	}

	public static void deleteIndex(String strTableName, String strColumnName) {
		File file = new File("data/" + strTableName + "." + strColumnName + ".index.class"); 
		if(file.delete()) 
		{ 
			System.out.println("Index deleted successfully.");
			if(!strColumnName.equals("TouchDate")) {
				//update meta
				try {
					ArrayList<String[]> lines = new ArrayList<String[]>();
					BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
					String line = "";
					while ((line = br.readLine()) != null) {
						String[] s = line.split(",");
						if(strTableName.equals(s[0]) && strColumnName.equals(s[1]))
							s[4] = "false"; 
						lines.add(s);
					}
					br.close();
	
					FileWriter w = new FileWriter("data/metadata.csv", false);
					for(int c=0; c<lines.size(); c++) {
						String[] s = lines.get(c);
						line = "";
						for(int d=0; d<s.length; d++) line += ("," + s[d]);
						w.write(line.substring(1) + "\n");
					}
					w.close();
				}
				catch(IOException e) {
					System.out.println("Failed to update metadata."); 
				}
			}
		}
		else { 
			System.out.println("Failed to delete the Index."); 
		}
	}

	public static void updateAllTableInd(String strTableName) throws DBAppException {	
		try {
			String line = null;			
			BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));

			while ((line = br.readLine()) != null) {		
				String[] content = line.split(",");
				if(strTableName.equals(content[0]) && content[4].equalsIgnoreCase("true")) {	
					Index.deleteIndex(strTableName, content[1]);
					if(content[2].equals("java.awt.Polygon"))
						Index.createRindex(strTableName, content[1]);
					else
						Index.createBindex(strTableName, content[1]);
				}
			}
			br.close();
			File file = new File("data/" + strTableName + ".TouchDate.index.class");
			if(file.exists()) {
				deleteIndex(strTableName, "TouchDate");
				createBindex(strTableName, "TouchDate");
			}
		} 
		catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("resource")
	public static String whichTreeToUse(String strTableName, String strColumnName) {
		
		try {
		
			String line = null;			
			BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));

			while ((line = br.readLine()) != null) {		
				String[] content = line.split(",");
				if(strTableName.equals(content[0]) && content[1].equals(strColumnName) && content[4].equalsIgnoreCase("true")) {	
					if(content[2].equals("java.awt.Polygon"))
						return "rtree";
					else
						return "bplustree";
				}
			}
			br.close();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		return "random word";
	}
}