package teamugh;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

public class CreationCheckers {

	public static boolean checkMeta() {
		File x = new File("data/metadata.csv");
		if(x.exists())
			return true;
		return false;
	}
	
	@SuppressWarnings("resource")
	public static boolean ExistTable(String strTableName)throws IOException
	{
		try
		{
			String line = null;
			BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
			while ((line = br.readLine()) != null) {
				String[] content = line.split(",");
				if (content[0].equals(strTableName))
				{
					return true;
				}
			}
			br.close();
		}
		catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return false;
	}
	
	public static void createMeta() throws IOException {
		File x = new File("data/metadata.csv");
		x.createNewFile();
	}
	
	public static boolean tableNotExists(DBApp engine, String strTableName) {
		for(Table table : engine.tables) {
			String name = table.getTableName();
			if(name.equals(strTableName))
				return false;
		}
		return true;
	}
	
	public static boolean checkKey(Hashtable<String,String> htblColNameType) {
		ArrayList<String> x = new ArrayList<String>();
		x.add("java.lang.Integer");
		x.add("java.lang.String");
		x.add("java.lang.Double");
		x.add("java.lang.Boolean");
		x.add("java.util.Date"); 
		x.add("java.awt.Polygon"); 
		
		Enumeration<String> y = (Enumeration<String>) htblColNameType.keys();
		
		while(y.hasMoreElements()) {
			String z = y.nextElement();
			if(!x.contains(htblColNameType.get(z))) {
				return false;
			}
		}
		return true;
	}
	
	@SuppressWarnings("unused")
	public static void addMeta(String strTableName,String clusteringKeyCol, Hashtable<String,String> htblColNameType ) throws DBAppException, IOException {
		if(!checkClusteringKeyValidity(clusteringKeyCol, htblColNameType)) {
			throw new DBAppException("Invalid clustering key");
		}
		File meta = new File("data/metadata.csv");
		FileWriter fileWriter = new FileWriter(meta, true);

		BufferedWriter bw = new BufferedWriter(fileWriter);
		PrintWriter out = new PrintWriter(bw);
		Enumeration<String> e = htblColNameType.keys();
		while(e.hasMoreElements()) {
			String columnName = e.nextElement();
			String colDataType = htblColNameType.get(columnName);
			String isClusteringKey = "";
			if(columnName.equals(clusteringKeyCol))
				isClusteringKey = "true";
			else
				isClusteringKey = "false";
			String isIndexed = "false";
			
			fileWriter.write(strTableName + "," + columnName + "," + colDataType + "," + isClusteringKey + "," + isIndexed + "\n");
			
		}
		//fileWriter.write(strTableName + ",TouchDate,java.util.Date,false,false\n");	//this messed everything up so let's not add it
		fileWriter.close();
	}
	
	public static boolean checkClusteringKeyValidity(String clusteringKeyCol, Hashtable<String,String> htblColNameType) {
		Enumeration <String> e = htblColNameType.keys();
		while(e.hasMoreElements()) {
			String k = e.nextElement();
			if(k.equals(clusteringKeyCol))
				return true;
		}
		return false;
	}
	
	public static void addtodirectory(String tablename, String path) throws DBAppException
	{
		File directory = new File(path + "/" + tablename);
		if(!directory.mkdir())
			throw new DBAppException();
	}
}
