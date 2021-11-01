package teamugh;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

public class DBApp{

	ArrayList<Table> tables;

	public DBApp() {

	}

	public void init() {			//loads already existing tables when initializing the DBApp
		File x = new File("data");
		File[] fileNameList = x.listFiles();
		this.tables = new ArrayList<Table>();
		for(int i = 0; i < fileNameList.length; i++) {
			if(fileNameList[i].isFile()) {
				String temp = fileNameList[i].getName();
				String[] extension = temp.split("\\.");		//the dot has to be skipped with backslashes like that!
				if(extension[1].equals("ser")) {
					Table t = Table.loadTable(extension[0]);
					this.tables.add(t);
				}
			}
		}
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,	Hashtable<String,String> htblColNameType )throws DBAppException {
		if (strTableName == null || strClusteringKeyColumn == null || htblColNameType == null) {
			throw new DBAppException();
		}
		if (!CreationCheckers.checkMeta()) {
			try {
				CreationCheckers.createMeta();
			}
			catch(IOException e) {
				e.printStackTrace();
			}
		}
		try {
			if(!CreationCheckers.ExistTable(strTableName))	{
				CreationCheckers.addtodirectory(strTableName, "data");
				Table t = new Table(strTableName,strClusteringKeyColumn,htblColNameType);
				t.saveTable();
				this.tables.add(t);
				System.out.println("Table created successfully!");
			}
			else {
				throw new DBAppException("A table with the same name already exists!");
			}
		} catch (IOException e) {
			System.out.println("Error creating table.");
		}
	}

	public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {
		if(strTableName == null || htblColNameValue == null) {
			throw new DBAppException();
		}

		if(InsertionCheckers.tableNotExists(this, strTableName)) {
			throw new DBAppException("The table you're trying to insert into does not exist");
		}

		if(!(InsertionCheckers.checkColumnNames(this, strTableName, htblColNameValue))) {
			throw new DBAppException("Invalid column names");
		}

		if(!(InsertionCheckers.checkKey(htblColNameValue, strTableName))) {
			throw new DBAppException("Invalid column data types");
		}

		//String clusteringKey = InsertionCheckers.getClusteringKeyColName(strTableName);
		if(Index.hasIndex(strTableName, UpdateCheckers.getTable(this, strTableName).getClusteringKey())) {
			try {
				InsertionCheckers.insertingUsingAnIndex(strTableName, htblColNameValue, UpdateCheckers.getTable(this, strTableName).getClusteringKey());
			} catch (ClassNotFoundException | IOException e) {
				System.out.println("Error inserting records. Please try again!");
			}
		}
		else {
			try {
				InsertionCheckers.insertionPositionAndPageNumber(this, strTableName, htblColNameValue);
			} catch (IOException e) {
				System.out.println("Error inserting records.");
			}
		}
	}

	public void updateTable(String strTableName, String strClusteringKey, Hashtable<String,Object> htblColNameValue ) throws DBAppException {
		if(strTableName == null || strClusteringKey == null || htblColNameValue == null) {
			throw new DBAppException();
		}
		if(UpdateCheckers.tableNotExists(this, strTableName)) {
			throw new DBAppException("The table you're trying to update does not exist");
		}
		if(!UpdateCheckers.checkColumnNames(this, strTableName, htblColNameValue)) {
			throw new DBAppException("Invalid column names");
		}
		if(!UpdateCheckers.checkDatatypes(strTableName, htblColNameValue)) {
			throw new DBAppException("Invalid column data types");
		}
		if(!UpdateCheckers.checkCorrespondingDatatypes(strTableName, htblColNameValue)) {
			throw new DBAppException("Invalid column data types");
		}
		if(Index.hasIndex(strTableName, UpdateCheckers.getTable(this, strTableName).getClusteringKey())) {
			System.out.println("update with index");
			if(UpdateCheckers.getTable(this, strTableName).getClusteringKeyType().equalsIgnoreCase("java.awt.Polygon")) {
				//System.out.println("RTree");
				UpdateCheckers.updateWithRTIndex(this, strTableName, strClusteringKey, htblColNameValue);
			}
			else {
				UpdateCheckers.updateWithBTIndex(this, strTableName, strClusteringKey, htblColNameValue);
			}
		}
		else {
			try {
				System.out.println("update without index");
				UpdateCheckers.update(this, strTableName, strClusteringKey, htblColNameValue);

			}
			catch(IOException e) {
				System.out.println("error");
			}
		}
	}

	public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {

		if(CreationCheckers.tableNotExists(this, strTableName)) {
			throw new DBAppException("The table you're trying to delete from does not exist");
		}
		if(!UpdateCheckers.checkDatatypes(strTableName, htblColNameValue)) {
			throw new DBAppException("A datatype you entered is not supported");
		}
		if(!UpdateCheckers.checkColumnNames(this, strTableName, htblColNameValue)) {
			throw new DBAppException("Invalid column names");
		}
		if(!UpdateCheckers.checkDatatypes(strTableName, htblColNameValue)) {
			throw new DBAppException("Invalid column data types");
		}
		if(!UpdateCheckers.checkCorrespondingDatatypes(strTableName, htblColNameValue)) {
			throw new DBAppException("Invalid column data types");
		}
		try {
			DeletionTakeTwo.compareAndDelete(this, DeletionTakeTwo.findIndex(strTableName, htblColNameValue), strTableName, htblColNameValue);
		}
		catch(IOException e) {
			System.out.println("error");
		}

	}

	public void createBTreeIndex(String strTableName, String strColName) throws DBAppException {
		if(UpdateCheckers.tableNotExists(this, strTableName)) {
			throw new DBAppException("There is no table with that name! check your spelling!");
		}
		if(!Index.checkCOlExists(strTableName, strColName)) {
			throw new DBAppException("There is no column with that name! check your spelling!");
		}
		if(Index.hasIndex(strTableName, strColName)) {
			throw new DBAppException("An index already exists on this column! Try another column!");
		}	
		try {
			Index.createBindex(strTableName, strColName);
		} catch (ClassNotFoundException | IOException e) {
			System.out.println("Failed to create B+ Tree index");
		}
	}

	public void createRTreeIndex(String strTableName, String strColName) throws DBAppException {	
		if(UpdateCheckers.tableNotExists(this, strTableName)) {
			throw new DBAppException("There is no table with that name! check your spelling!");
		}
		if(!Index.checkCOlExists(strTableName, strColName)) {
			throw new DBAppException("There is no column with that name! check your spelling!");
		}
		if(Index.hasIndex(strTableName, strColName)) {
			throw new DBAppException("An index already exists on this column! Try another column!");
		}
		try {
			Index.createRindex(strTableName, strColName);
		} catch (ClassNotFoundException | IOException e) {
			System.out.println("Failed to create R+ Tree index");
		}
	}

	@SuppressWarnings("rawtypes")
public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException { 
 		
		if(arrSQLTerms == null || strarrOperators == null) {
			throw new DBAppException("One of the entered parameters is empty! Enter all required fields");
		}		
		for(int i = 0; i < arrSQLTerms.length; i++) {
			if(UpdateCheckers.tableNotExists(this, arrSQLTerms[i]._strTableName)) {
				throw new DBAppException("There is no table with the name " + arrSQLTerms[i]._strTableName 
						+ " check your spelling!");
			}
		}	
		if(!SelectStuff.checkColumnNames(this, arrSQLTerms)) {
			throw new DBAppException("A column name you have entered does not match any of the columns in this table!");
		}		
		if(SelectStuff.checkOperators(strarrOperators) == 2) { 
			throw new DBAppException("An operator you have entered does not match the supported operator types! "
					+ "Supported types include AND, OR, and XOR only!");
		}
		else if(SelectStuff.checkOperators(strarrOperators) == 1) {
			if(!(strarrOperators.length == 0)) {
				throw new DBAppException("SQLTerms and the number of SQL Terms operators does not match");
			}
		}
				
		ArrayList<ArrayList<Object[]>> x = new ArrayList<ArrayList<Object[]>>(); 	//contains the result of each col cond
		for(int i = 0; i < arrSQLTerms.length; i++) {
			String _strTableName = arrSQLTerms[i]._strTableName;
			String  _strColumnName = arrSQLTerms[i]._strColumnName;
			String _strOperator = arrSQLTerms[i]._strOperator;
			Object _objValue = arrSQLTerms[i]._objValue;
			

			try {
				if(SelectStuff.colHasIndex(_strColumnName, _strTableName) == false) {
					x.add(SelectStuff.strOperatorType(this, _strTableName, _strColumnName, _strOperator, _objValue));
				}
				else {
					x.add(SelectStuff.indexedStrOperatorType(this, _strTableName, _strColumnName, _strOperator, _objValue));
				}
			}
			catch(Exception e) {
				System.out.println("Wrong input");
			}			
			//get col type
			//if polygon
			//manually compare
		}
		for(int i = 0; i < x.size(); i++) {
			System.out.println(i + " result size is " + x.get(i).size());
		}

		if(SelectStuff.checkOperators(strarrOperators) == 1) {
			System.out.println("Result Set Size: " + x.get(0).size());
			Iterator<Object[]> iterator = x.get(0).listIterator();
			while(iterator.hasNext()) {
				Object[] i = iterator.next();
				for (int t = 0; t < i.length; t++) {
					System.out.print(i[t] + " ");
				}
				System.out.println("");

			}	
			return iterator;
		}
				try {

					ArrayList<Object[]> finalResult =  SelectStuff.strarrOperatorType(this, strarrOperators, x);
					System.out.println("Result Set Size:" + finalResult.size());
					Iterator<Object[]> iterator = finalResult.listIterator();
		
					while(iterator.hasNext()) {
						Object[] i = iterator.next();
						for (int t = 0; t < i.length; t++) {
							System.out.print(i[t] + " ");
						}
						System.out.println("");
		
					}				
					return iterator;
				}
				catch (Exception e) {
					System.out.print("Wrong Input");
				}
		
				return null;
	}
}