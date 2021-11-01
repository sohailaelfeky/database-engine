package teamugh;

import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;

import bPlusTree.Ref;
import rTree.ROverflowPage;
import rTree.RRef;
import rTree.RTree;
import bPlusTree.BPTree;
import bPlusTree.OverflowPage;

public class DeletionTakeTwo {

	//	public static void main(String[] args) throws IOException, DBAppException {
	//	
	//		Hashtable<String,Object> htblColNameValue = new Hashtable<String,Object>();
	//		htblColNameValue.put("id","carol");
	//
	//		int[] x = findIndex("First Test", htblColNameValue);
	//		
	//		for(int i = 0; i < x.length; i++) {
	//			System.out.println("result" + x[i]);
	//			}
	//		
	////		String x = indexType(2);
	////		System.out.println(x);
	//		}

	@SuppressWarnings("resource")
	public static int[] findIndex(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {
		ArrayList<Integer> x1 = new ArrayList<Integer>();
		Enumeration<String> y = htblColNameValue.keys();
		String[] ht = new String[htblColNameValue.size()];
		int c = 0;
		while(y.hasMoreElements()) {
			String z = y.nextElement();
			ht[c] = z;
			c++;
		}
		try {
			String line = null;			
			BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
			int ind = 0;
			while ((line = br.readLine()) != null) {		
				String[] content = line.split(",");
				for(int i = 0; i < ht.length; i++) {
					if(strTableName.equals(content[0]) && ht[i].equals(content[1])) {
						x1.add(ind);
					}
				}
				if(strTableName.equals(content[0]))
					ind++;
			}
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int[] x = new int[x1.size()];
		for(int i = 0; i < x1.size(); i++) {
			x[i] = x1.get(i);		
		}
		return x;
	}

	@SuppressWarnings("resource")
	public static String indexType(int x, String strTableName) {
		int c = 0;
		String type = null;
		try {	
			String line = null;			
			BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
			while ((line = br.readLine()) != null) {		
				String[] content = line.split(",");
				if(strTableName.equals(content[0]) && c == x) {
					return content[2];
				}
				if(strTableName.equals(content[0]))
					c++;
			}	
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return type;
	}


	public static void compareAndDelete(DBApp engine, int[] index, String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException {

		Table table = UpdateCheckers.getTable(engine, strTableName);
		Object[] htValues = new Object[htblColNameValue.size()];
		Object[] htColNames = new Object[htblColNameValue.size()];
		ArrayList<String> htIndexes = new ArrayList<String>(); //arrayList of indexed columns

		Enumeration<String> y = htblColNameValue.keys();	//the column names
		int c = 0;
		while(y.hasMoreElements()) {
			String z = y.nextElement();
			htValues[c] = htblColNameValue.get(z);    //column values
			htColNames[c] = z;
			c++;
		}

		//checking if table is empty
		int pages = table.getNoOfPages();
		if(pages == 0) {
			throw new DBAppException("Table is Empty");
		}

		//arrayList of indexed columns
		for(int i = 0 ; i < htColNames.length ; i++) {
			if(Index.hasIndex((String) htColNames[i], strTableName)) {
				htIndexes.add((String)htColNames[i]);
			}
		}
		//if columns has no indexes

		if(htIndexes.size()==0) {
			for(int i = 0; i < pages; i++) {	//loop each page
				Page p = Page.loadPage(strTableName, i+1);	//get the page
				for(int j = 0; j < p.size(); j++) { //loop each row in each page
					Object[] array = p.get(j); //object in the vector
					boolean delete = true;
					for(int cp = 0; cp < index.length; cp++) {
						String compVal = indexType(index[cp], strTableName);
						System.out.println(Arrays.toString(index));
						int compResult;
						switch(compVal) { 
						//compare the ck of the middle element of the vector with the input
						case "java.lang.Integer":
							Integer i1 =((Integer) htValues[cp]);
							Integer i2 = (Integer) array[index[cp]];
							compResult = i1.compareTo(i2);
							break;
						case "java.lang.String":
							String s1 =((String) htValues[cp]);
							String s2 = (String) array[index[cp]]; 
							compResult = s1.compareTo(s2);
							break;
						case "java.lang.Double":
							Double d1 =((Double) htValues[cp]);
							Double d2 = (Double) array[index[cp]];
							compResult = d1.compareTo(d2);
							break;
						case "java.awt.Polygon": 
							Polygon p1 = ((Polygon) htValues[cp]);
							Polygon p2 = ((Polygon) array[index[cp]]);
							boolean equal = ParsePolygon.compareByCoordinates(p1, p2);
							if(equal)
								compResult = 0;
							else
								compResult = 1;
							break;
						case "java.lang.Boolean":  
							Boolean b1 =((Boolean) htValues[cp]);
							Boolean b2 = (Boolean) array[index[cp]];
							compResult = b1.compareTo(b2);
							break;
						case "java.util.Date" :  
							Integer d3 =((Integer) htValues[cp]);
							Integer d4 = (Integer) array[index[cp]];
							compResult = d3.compareTo(d4);
							break;
						default:
							throw new DBAppException();
						}					  
						if(compResult != 0) {
							delete = false;										
						}
					}
					if(delete == true) {
						p.remove(j);
						j--;
					}
				}
				p.savePage(strTableName, i+1);
			}
		}

		//if columns has indexes
		else {
			// table 
			Table b = UpdateCheckers.getTable(engine, strTableName);

			//arraylists holding references
			ArrayList<Integer> NodesToBeDeletedPages = new ArrayList<Integer>();
			ArrayList<Integer> NodesToBeDeletedIndices = new ArrayList<Integer>();
			
			if((Index.whichTreeToUse(strTableName,htIndexes.get(0))).equalsIgnoreCase("rtree")) {
				//rtree code
				System.out.println("using RTree");
				deleteWithRTree(b,strTableName,htIndexes,htValues,htColNames,NodesToBeDeletedPages,NodesToBeDeletedIndices);
			}
			
			else {
				//BPTree
				System.out.println("using BPTree");
				deleteWithBPTree(b,strTableName,htIndexes,htValues,htColNames,NodesToBeDeletedPages,NodesToBeDeletedIndices);
			} 
		}

		//delete empty pages after we're done
		for(int i = 0;i<pages;i++) {
			Page page = Page.loadPage(strTableName, i+1);
			if(page.isEmpty() || page.get(0) == null) {
				table.removePage();
				Page.deletePage(table.getTableName(), i+1);
			}
		}
		table.renamePages();
		table.saveTable();

		//recreate all indices
		Index.updateAllTableInd(strTableName);
	}
	
	@SuppressWarnings({"rawtypes", "unchecked"})
	public static void deleteWithRTree(Table b,String strTableName, ArrayList<String> htIndexes, Object[] htValues, Object[] htColNames , ArrayList<Integer> NodesToBeDeletedPages, ArrayList<Integer> NodesToBeDeletedIndices) throws DBAppException {
		//getting the tree of any indexed column
		RTree T = Index.loadRTIndex(strTableName, htIndexes.get(0));

		//adding all Refs with the specified value to the arraylist
		for (int j = 0; j < htColNames.length; j++) {
			if (htIndexes.get(0).equals(htColNames[j])) {
				RRef r = T.search((Polygon) htValues[j]);
				
				if(r == null) {
					throw new DBAppException("The record you are trying to delete does not exist in this table.");
				}

				if(r instanceof ROverflowPage) {
					ROverflowPage RoverflowPage = (ROverflowPage) r;
					while(RoverflowPage!=null) {			//NOTE: there can be more than one overflow page
						ArrayList<RRef> overflowref = RoverflowPage.getRecords();
						for(int p = 0 ; p< overflowref.size(); p++) {
							NodesToBeDeletedPages.add(overflowref.get(p).getPage());
							NodesToBeDeletedIndices.add(overflowref.get(p).getIndexInPage());
						}
						RoverflowPage = RoverflowPage.getNext();
					}
				}
				else {
					NodesToBeDeletedPages.add(r.getPage());
					NodesToBeDeletedIndices.add(r.getIndexInPage());
				}
				break;
			}
		}
//		for(int l = 0 ;l<NodesToBeDeletedPages.size();l++)
//			System.out.print("("+NodesToBeDeletedPages.get(l)+", "+NodesToBeDeletedIndices.get(l)+")"+", ");
//		System.out.println();
		//looping on the NodesToBeDeleted array(s) to access records in the table to check other filters
		//for(int i = 0 ; i < NodesToBeDeletedPages.size(); i++)
		while(NodesToBeDeletedPages.size()>0) {
			//Ref f = (NodesToBeDeleted.get(i));
			int pageNumber= NodesToBeDeletedPages.get(0);
			int recordNumber = NodesToBeDeletedIndices.get(0);
			//loading page
			Page page = Page.loadPage(strTableName, (pageNumber+1));
			//accessing the record
			Object[] record = page.get(recordNumber);

			//checking the record values with the hashtable filters
			ArrayList<String> tableColNames = b.getColNames(); //table's col names

			//create an arraylist of positions need to be checked!
			ArrayList<Integer> positions = new ArrayList<Integer>();
			for(int pos = 0 ; pos < tableColNames.size(); pos++) {
				for(int vpos = 0 ; vpos < htColNames.length; vpos++) {
					if(tableColNames.get(pos).equals(htColNames[vpos])) {
						positions.add(pos);
					}
				}
			}

			//checking the hashtables values :')
			boolean checker = false; //to check if the whole record is as needed
			for(int m = 0 ; m < htColNames.length; m++) //looping on hashtable column names
			{
				for(int n = 0 ; n < positions.size(); n++) //looping on table's positions
				{
					if(htColNames[m].equals(tableColNames.get(positions.get(n)))) //checking whether the hashtable columns names == to tables col names
					{
						//System.out.println(htColNames[m] + " " + tableColNames.get(positions.get(n)));
						if(Index.columnType(strTableName, tableColNames.get(positions.get(n))).equals("Polygon")) {
							//System.out.println(ParsePolygon.toString(htValues[m]) + "\t" + ParsePolygon.toString(record[positions.get(n)]));
							if(ParsePolygon.compareByCoordinates(htValues[m], record[positions.get(n)])) {
								checker = true;
							}
							else {
								checker = false;
								m = htColNames.length + 1;
								break;
							}
						}
						else {
							//System.out.println(htValues[m] + " " + record[positions.get(n)]);
							if(htValues[m].equals(record[positions.get(n)])) //checking whether hastable values == table values
							{
								checker = true;
							}
	
							else {
								checker = false;
								m = htColNames.length + 1; // so we won't enter the first loop again
								break; //to break the second loop
							}
						}
					}
				}
			}
			//checking whether the record satisfies the filters or not
			if (checker) {
				page.remove(recordNumber); //delete from table
				try {
					page.savePage(strTableName, (pageNumber+1));
				} catch (IOException e) {
					e.printStackTrace();
				}
				//remove the page number and corresponding index we deleted
				NodesToBeDeletedPages.remove(0);
				NodesToBeDeletedIndices.remove(0);
				//decrementing indices
				for(int k=0;k<NodesToBeDeletedPages.size() && NodesToBeDeletedPages.get(k) == pageNumber;k++) {		//pageNumber of the record we already removed
					int r = NodesToBeDeletedIndices.get(k);
					NodesToBeDeletedIndices.remove(k);
					r--;
					NodesToBeDeletedIndices.add(k, r);
				}
			}
			else {
				NodesToBeDeletedPages.remove(0); 
				NodesToBeDeletedIndices.remove(0);
			}
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void deleteWithBPTree(Table b, String strTableName, ArrayList<String> htIndexes, Object[] htValues, Object[] htColNames , ArrayList<Integer> NodesToBeDeletedPages, ArrayList<Integer> NodesToBeDeletedIndices) throws DBAppException {
		//getting the tree of any indexed column
		BPTree T = Index.loadBTIndex(strTableName, htIndexes.get(0));

		//adding all Refs with the specified value to the arraylist
		for ( int j = 0; j < htColNames.length; j++) {
			if ( htIndexes.get(0).equals(htColNames[j])) {
				Ref r = (Ref) T.search((Comparable)htValues[j]);
				
				if(r == null) {
					throw new DBAppException("The record you are trying to delete does not exist in this table.");
				}

				if(r instanceof OverflowPage) {
					OverflowPage overflowPage = (OverflowPage) r;
					while(overflowPage!=null) {			//NOTE: there can be more than one overflow page
						ArrayList<bPlusTree.Ref> overflowref = overflowPage.getRecords();
						for(int p = 0 ; p< overflowref.size(); p++) {
							NodesToBeDeletedPages.add(overflowref.get(p).getPage());
							NodesToBeDeletedIndices.add(overflowref.get(p).getIndexInPage());
						}
						overflowPage = overflowPage.getNext();
					}
				}
				else {
					NodesToBeDeletedPages.add(r.getPage());
					NodesToBeDeletedIndices.add(r.getIndexInPage());
				}
				break;
			}
		}
		//looping on the NodesToBeDeleted array(s) to access records in the table to check other filters
		//for(int i = 0 ; i < NodesToBeDeletedPages.size(); i++)
		while(NodesToBeDeletedPages.size()>0) {
			int pageNumber= NodesToBeDeletedPages.get(0);
			int recordNumber = NodesToBeDeletedIndices.get(0);
			//loading page
			Page page = Page.loadPage(strTableName, (pageNumber+1));
			//accessing the record
			Object[] record = page.get(recordNumber);
			//checking the record values with the hashtable filters
			ArrayList<String> tableColNames = b.getColNames(); //table's col names
			//create an arraylist of positions need to be checked!
			ArrayList<Integer> positions = new ArrayList<Integer>();
			for(int pos = 0 ; pos < tableColNames.size(); pos++) {
				for(int vpos = 0 ; vpos < htColNames.length; vpos++) {
					if(tableColNames.get(pos).equals(htColNames[vpos])) {
						positions.add(pos);
					}
				}
			}

			//checking the hashtables values :')
			boolean checker = false; //to check if the whole record is as needed
			for(int m = 0 ; m < htColNames.length; m++) //looping on hashtable column names
			{
				for(int n = 0 ; n < positions.size(); n++) //looping on table's positions
				{
					if(htColNames[m].equals(tableColNames.get(positions.get(n)))) //checking whether the hashtable columns names == to tables col names
					{
						if(htValues[m].equals(record[positions.get(n)])) //checking whether hastable values == table values
						{		
							checker = true;
						}
						else {
							checker = false;
							m = htColNames.length + 1; // so we won't enter the first loop again
							break; //to break the second loop
						}
					}
				}
			}
			//System.out.println(checker);
			//checking whether the record satisfies the filters or not
			if (checker) {
				page.remove(recordNumber); //delete from table
				try {
					page.savePage(strTableName, (pageNumber+1));
				} catch (IOException e) {
					e.printStackTrace();
				}
				//remove the page number and corresponding index we deleted
				NodesToBeDeletedPages.remove(0);
				NodesToBeDeletedIndices.remove(0);
				//decrementing indices
				for(int k=0;k<NodesToBeDeletedPages.size() && NodesToBeDeletedPages.get(k) == pageNumber;k++) {		//pageNumber of the record we already removed
					int r = NodesToBeDeletedIndices.get(k);
					NodesToBeDeletedIndices.remove(k);
					r--;
					NodesToBeDeletedIndices.add(k, r);
				}
			}
			else {
				NodesToBeDeletedPages.remove(0); 
				NodesToBeDeletedIndices.remove(0);
			}
		}
	}
}