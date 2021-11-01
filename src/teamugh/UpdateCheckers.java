package teamugh;

import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import bPlusTree.BPTree;
import bPlusTree.OverflowPage;
import bPlusTree.Ref;
import bPlusTree.Reference;
import rTree.ROverflowPage;
import rTree.RRef;
import rTree.RTree;

public class UpdateCheckers {
	
	public static boolean tableNotExists(DBApp engine, String strTableName) {
		return CreationCheckers.tableNotExists(engine, strTableName);
	}

	public static boolean checkDatatypes(String strTableName, Hashtable<String,Object> htblColNameValue) {
		ArrayList<String> columnNames = new ArrayList<String>(htblColNameValue.keySet());
		ArrayList<String> inputDatatypes = new ArrayList<String>();
		for(String col : columnNames) {
			inputDatatypes.add((htblColNameValue.get(col).getClass().getName()));
		}
		ArrayList<String> supportedDatatypes = new ArrayList<String>();
		supportedDatatypes.add("java.lang.Integer");
		supportedDatatypes.add("java.lang.String");
		supportedDatatypes.add("java.lang.Double");
		supportedDatatypes.add("java.lang.Boolean");
		supportedDatatypes.add("java.util.Date"); 
		supportedDatatypes.add("java.awt.Polygon");
		for(String type : inputDatatypes) {
			if(!supportedDatatypes.contains(type))
				return false;
		}
		return true;
	}
	
	public static boolean checkColumnNames(DBApp engine, String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {
		Table table = getTable(engine, strTableName);
		if(table == null) {
			throw new DBAppException("The table you are trying to update does not exist.");
		}
		ArrayList<String> inputColumnNames = new ArrayList<String>(htblColNameValue.keySet());
		ArrayList<String> tableColumnNames = table.getColNames();
		for(String inputName : inputColumnNames) {
			if(!tableColumnNames.contains(inputName))
				return false;
		}
		return true;
	}

	@SuppressWarnings("resource")
	public static boolean checkCorrespondingDatatypes(String strTableName, Hashtable<String,Object> htblColNameValue) {
		try {
			ArrayList<String> columnNames = new ArrayList<String>(htblColNameValue.keySet());
			ArrayList<String> inputDatatypes = new ArrayList<String>();
			for(String col : columnNames) {
				inputDatatypes.add((htblColNameValue.get(col)).getClass().getName());
			}
			BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");
				if (values[0].equals(strTableName)) {
					if(columnNames.contains(values[1])) {
						int correspondingIndex = columnNames.indexOf(values[1]);
						if(!inputDatatypes.get(correspondingIndex).equalsIgnoreCase(values[2])) {
							return false;
						}
					}	
				}
			}
			br.close();
			return true;
		}
		catch (IOException e) {
			return false;
		}		
	}
	
	public static Table getTable(DBApp engine, String strTableName) {
		Table table = null;
		for(Table t : engine.tables) {
			if(t.getTableName().equals(strTableName)) {
				table = t;
				break;
			}
		}
		return table;
	}

	public static void update(DBApp engine, String strTableName, String strClusteringKey, Hashtable<String,Object> htblColNameValue) throws IOException, DBAppException {
		/**find records to update
				find page that contains the first instance
				cases:
					all the instances are in the same page (check the max: if == input clustering key, check the next page. if min == input, check previous page)
							checking the next page: if last page, there's no need to check anything further
					instances span more than one page: we find that out by checking the max as mentioned above
				get the instances indices in an array of pairs: first number is the page index, second is the record index
		update the records : replace the values
		sorting: 
			if clustering key was updated:
				part 1: remove the updated records from the table's pages, THEN re-insert them using the original insertion method (done)
				part 2: remove the updated records from the table's pages, shift the records backwards, then re-insert the updated records using the original insertion method
		 * 
		 */
		Table b = getTable(engine, strTableName);
		if(b == null) {
			throw new DBAppException("The table you are trying to update does not exist.");
		}
		Object[] result = findRecords(b, strClusteringKey, htblColNameValue);
		if(result[2].equals(false)) {	//clustering key not updated
			int[] first = (int[]) result[0];
			int[] last = (int[]) result[1];
			int ckIndex = (int) result[3];
			String ckType = (String) result[4];
			Object ck = result[5];
			replaceValuesWithoutSorting(b, first, last, ckIndex, ckType, ck, htblColNameValue);
		}
		else {	//clustering key updated
			int[] first = (int[]) result[0];
			int[] last = (int[]) result[1];
			int ckIndex = (int) result[3];
			String ckType = (String) result[4];
			Object ck = result[5];
			replaceValuesAndSort(engine, b, first, last, ckIndex, ckType, ck, htblColNameValue);
		}
	}
	
	public static Object[] findRecords(Table table, String strClusteringKey, Hashtable<String,Object> htblColNameValue) throws IOException, DBAppException {
		Object[] ckInfo = InsertionCheckers.clusteringKeyPosition(table.getTableName());
		int ckIndex = 0;
		String ckType="";
		if(ckInfo[0]!=null) {
			ckIndex = (int) ckInfo[0];
			ckType= (String) ckInfo[1];
		}
		else {
			throw new DBAppException("No clustering key is defined for this table.");
		}
		//make a dummy record to use the same pageToInsertInto() method
		Object parse = parseString(strClusteringKey, ckType);
		Object[] rec = new Object[ckIndex+1];
		rec[ckIndex] = parse;
		int pIndex = InsertionCheckers.pageToInsertInto(table.getTableName(), rec, 0, table.getNoOfPages()-1, ckIndex, ckType);
		if(pIndex==-1) {
			throw new DBAppException("The table you are trying to update does not have any records.");
		}
		Page p = Page.loadPage(table.getTableName(), (pIndex+1));
		int rIndex = searchInPage(p, rec, 0, p.size()-1, ckIndex, ckType);
		if(rIndex==-1) {
			throw new DBAppException("The table you are trying to update does not have any records with the value " + strClusteringKey);
		}
		int [][] range = searchRange(table, pIndex, rIndex, ckIndex, ckType, parse);
		int[] firstOccurance = range[0];
		int[] lastOccurance = range[1];
		ArrayList<String> inputColumns = new ArrayList<String>(htblColNameValue.keySet());
		if(!inputColumns.contains(table.getColNames().get(ckIndex)))
			return new Object[]{firstOccurance, lastOccurance, false, ckIndex, ckType, parse};	//clustering key not updated
		return new Object[]{firstOccurance, lastOccurance, true, ckIndex, ckType, parse};	//clustering key updated
	}

	public static int searchInPage(Page vec, Object[] x, int l, int r, int ck, Object type) throws DBAppException {
		String t = (String) type;
		int compResult;
		if (r==l) {
			return r;
		}
		if (r>l) {
			int mid = l + (r - l)/2; 
			Object[] o = vec.get(mid);
			switch(t) {					//compare the ck of the middle element of the vector with the input
			case "java.lang.Integer":
				Integer i1 =((Integer) x[ck]);
				Integer i2 = (Integer) o[ck];
				compResult = i1.compareTo(i2);
				break;
			case "java.lang.String":
				String s1 =((String) x[ck]);
				String s2 = (String) o[ck];
				compResult = s1.compareTo(s2);
				break;
			case "java.lang.Double":
				Double d1 =((Double) x[ck]);
				Double d2 = (Double) o[ck];
				compResult = d1.compareTo(d2);
				break;
			case "java.awt.Polygon": 
				Polygon p1 = (Polygon) x[ck];
				Polygon p2 = (Polygon) o[ck];
				Double d = (double) (p1.getBounds().getSize().width * p1.getBounds().getSize().height);
				Double c = (double) (p2.getBounds().getSize().width * p2.getBounds().getSize().height);
				compResult = d.compareTo(c);
				break;
			case "java.lang.Boolean":  
				Boolean b1 =((Boolean) x[ck]);
				Boolean b2 = (Boolean) o[ck];
				compResult = b1.compareTo(b2);
				break;
			case "java.util.Date" :  
				Integer d3 =((Integer) x[ck]);
				Integer d4 = (Integer) o[ck];
				compResult = d3.compareTo(d4);
				break;
			default:
				throw new DBAppException();
			}
			if (compResult == 0) { // If the element is present at the middle itself
				return mid;
			}
			if (compResult > 0) {
				return searchInPage(vec, x, mid+1, r, ck, type); 
			}
			if (compResult < 0) {
				return searchInPage(vec, x, l, mid-1, ck, type);  
			}
		} 
		// We reach here when element is not present
		return -1; 
	}

	public static int[] searchFirstOccurance(Table table, int pIndex, int rIndex, int ckIndex, String ckType, Object ck) throws DBAppException {
		Page page = Page.loadPage(table.getTableName(), pIndex+1);
		Object[] rec = new Object[ckIndex+1];
		rec[ckIndex] = ck;
		while(pIndex>=0) {
			while(rIndex>=0 && InsertionCheckers.compareInput(page.get(rIndex)[ckIndex], rec[ckIndex], ckType) == 0) {
				rIndex--;
				if(rIndex>=0 && InsertionCheckers.compareInput(page.get(rIndex)[ckIndex], rec[ckIndex], ckType) != 0) {
					return new int[] {pIndex,rIndex+1};
				}
			}
			pIndex--;
			if(pIndex==-1) {
				if(InsertionCheckers.compareInput(page.get(0)[ckIndex], rec[ckIndex], ckType) == 0)
					return new int[] {0,0};
				else {
					throw new DBAppException("The table you are trying to update does not have any records with the value you input.");
				}
			}
			page = Page.loadPage(table.getTableName(), pIndex+1);
			rIndex = page.size()-1;

			if(InsertionCheckers.compareInput(page.get(rIndex)[ckIndex], rec[ckIndex], ckType) != 0) {
				return new int[] {pIndex+1,0};
			}
		}
		return new int[] {pIndex, rIndex};
	}

	public static int[] searchLastOccurance(Table table, int pIndex, int rIndex, int ckIndex, String ckType, Object ck) throws DBAppException {
		Page page = Page.loadPage(table.getTableName(), pIndex+1);
		Object[] rec = new Object[ckIndex+1];
		rec[ckIndex] = ck;
		while(pIndex<=table.getNoOfPages()-1) {
			while(rIndex<=page.size()-1 && InsertionCheckers.compareInput(page.get(rIndex)[ckIndex], rec[ckIndex], ckType) == 0) {
				rIndex++;
				if(rIndex<=page.size()-1 && InsertionCheckers.compareInput(page.get(rIndex)[ckIndex], rec[ckIndex], ckType) != 0) {
					return new int[] {pIndex,rIndex-1};
				}
			}
			pIndex++;
			if(pIndex==table.getNoOfPages()) {
				if(InsertionCheckers.compareInput(page.get(page.size()-1)[ckIndex], rec[ckIndex], ckType) == 0)
					return new int[] {table.getNoOfPages()-1,page.size()-1};
				else {
					throw new DBAppException("The table you are trying to update does not have any records with the value you input.");
				}
			}
			page = Page.loadPage(table.getTableName(), pIndex+1);
			rIndex = 0;
			if(InsertionCheckers.compareInput(page.get(rIndex)[ckIndex], rec[ckIndex], ckType) != 0) {
				page = Page.loadPage(table.getTableName(), pIndex);
				return new int[] {pIndex-1, page.size()-1};
			}
		}
		return new int[] {pIndex, rIndex};
	}

	public static int[][] searchRange(Table table, int pIndex, int rIndex, int ckIndex, String ckType, Object ck) throws DBAppException {
		int[] firstOccurance = searchFirstOccurance(table, pIndex, rIndex, ckIndex, ckType, ck);
		int[] lastOccurance = searchLastOccurance(table, pIndex, rIndex, ckIndex, ckType, ck);
		return new int[][] { firstOccurance, lastOccurance };
	}

	public static void replaceValuesWithoutSorting(Table table, int[] first, int[] last, int ckIndex, String ckType, Object ck, Hashtable<String,Object> htblColNameValue) throws IOException {
		System.out.println("replace values without sorting");
		boolean polygon = false;
		if(ck instanceof Polygon)
			polygon = true;
		
		int pIndexF = first[0];
		int rIndexF = first[1];
		int pIndexL = last[0];
		int rIndexL = last[1];
		Enumeration<String> e = htblColNameValue.keys();
		ArrayList<Integer> updateIndices = new ArrayList<Integer>();
		ArrayList<Object> updateValues = new ArrayList<Object>();
		while(e.hasMoreElements()) {
			String colName = e.nextElement();
			int i = table.getColNames().indexOf(colName);
			updateIndices.add(i);
			updateValues.add(htblColNameValue.get(colName));
		}
		while(pIndexF<pIndexL) {
			Page page = Page.loadPage(table.getTableName(), pIndexF+1);
			while(rIndexF<=page.size()-1) {
				boolean cont = true;
				if(polygon) {
					if(!ParsePolygon.compareByCoordinates(ck, page.get(rIndexF)[ckIndex])) {
						cont= false;
						rIndexF++;
					}
				}
				if(cont) {
					for(int i=0;i<updateIndices.size();i++) {
						int indexToUpdate = (int) updateIndices.get(i);
						Object newValue = updateValues.get(i);
						page.get(rIndexF)[indexToUpdate] = newValue;
					}
					page.get(rIndexF)[page.get(rIndexF).length-1] = new Date();
					rIndexF++;
				}
			}
			page.savePage(table.getTableName(), pIndexF+1);
			pIndexF++;
			rIndexF = 0;
		}
		Page page = Page.loadPage(table.getTableName(), pIndexF+1);
		while(rIndexF <= rIndexL) {
			boolean cont = true;
			if(polygon) {
				if(!ParsePolygon.compareByCoordinates(ck, page.get(rIndexF)[ckIndex])) {
					cont= false;
					rIndexF++;
				}
			}
			if(cont) {
				for(int i=0;i<updateIndices.size();i++) {
					int indexToUpdate = (int) updateIndices.get(i);
					Object newValue = updateValues.get(i);
					page.get(rIndexF)[indexToUpdate] = newValue;
				}
				page.get(rIndexF)[page.get(rIndexF).length-1] = new Date();
				rIndexF++;
			}
		}
		page.savePage(table.getTableName(), pIndexF+1);
		table.saveTable();
	}
	
	public static void replaceValuesAndSort(DBApp engine, Table table, int[] first, int[] last, int ckIndex, String ckType, Object ck, Hashtable<String,Object> htblColNameValue) throws IOException {
		//System.out.println("replace values and sort");
		boolean polygon = false;
		if(ck instanceof Polygon)
			polygon = true;
		int pIndexF = first[0];
		int rIndexF = first[1];
		int pIndexL = last[0];
		int rIndexL = last[1];
		System.out.println(Arrays.toString(first) + "  " + Arrays.toString(last));
		Page page = Page.loadPage(table.getTableName(), pIndexF+1);
		Object[] rec = new Object[ckIndex+1];
		rec[ckIndex] = ck;
		Enumeration<String> e = htblColNameValue.keys();
		ArrayList<Integer> updateIndices = new ArrayList<Integer>();
		ArrayList<Object> updateValues = new ArrayList<Object>();
		Vector<Object[]> updatedRecords = new Vector<Object[]>();
		while(e.hasMoreElements()) {
			String colName = e.nextElement();
			int i = table.getColNames().indexOf(colName);
			updateIndices.add(i);
			updateValues.add(htblColNameValue.get(colName));
		}
		while(pIndexF<pIndexL) {
			page = Page.loadPage(table.getTableName(), pIndexF+1);
			while(rIndexF<=page.size()-1) {
				boolean cont = true;
				if(polygon) {
					if(!ParsePolygon.compareByCoordinates(ck, page.get(rIndexF)[ckIndex])) {
						cont= false;
						rIndexF++;
					}
				}
				if(cont) {
					for(int i=0;i<updateIndices.size();i++) {
						int indexToUpdate = (int) updateIndices.get(i);
						Object newValue = updateValues.get(i);
						page.get(rIndexF)[indexToUpdate] = newValue;
					}
					Object[] updatedRecord = page.get(rIndexF);
					page.remove(rIndexF);
					Object[] updatedRecordWithoutDate = new Object[updatedRecord.length-1];
					for(int i=0;i<updatedRecordWithoutDate.length;i++) {
						updatedRecordWithoutDate[i] = updatedRecord[i];
					}
					updatedRecords.add(updatedRecordWithoutDate);
				}
			}
			page.savePage(table.getTableName(), pIndexF+1);
			pIndexF++;
			rIndexF = 0;
		}
		page = Page.loadPage(table.getTableName(), pIndexF+1);
		while(rIndexF<=rIndexL) {
			boolean cont = true;
			if(polygon) {
				if(!ParsePolygon.compareByCoordinates(ck, page.get(rIndexF)[ckIndex])) {
					cont= false;
					rIndexF++;
				}
			}
			if(cont) {
				for(int i=0;i<updateIndices.size();i++) {
					int indexToUpdate = (int) updateIndices.get(i);
					Object newValue = updateValues.get(i);
					page.get(rIndexF)[indexToUpdate] = newValue;
				}
				Object[] updatedRecord = page.get(rIndexF);
				page.remove(rIndexF);
				Object[] updatedRecordWithoutDate = new Object[updatedRecord.length-1];
				for(int i=0;i<updatedRecordWithoutDate.length;i++) {
					updatedRecordWithoutDate[i] = updatedRecord[i];
				}
				updatedRecords.add(updatedRecordWithoutDate);
				rIndexL--;
			}
		}
		page.savePage(table.getTableName(), pIndexF+1);
		table.saveTable();
		Vector<Hashtable<String,Object>> updatedRecsAsHashtables = new Vector<Hashtable<String,Object>>();
		while(updatedRecords.size()>0) {
			Object[] updatedRecord = updatedRecords.get(0);
			Hashtable<String,Object> record = new Hashtable<String,Object>();
			for(int i =0;i<updatedRecord.length;i++) {
				record.put(table.getColNames().get(i), updatedRecord[i]);
			}
			updatedRecsAsHashtables.add(record);
			updatedRecords.remove(0);
		}
		EmptySpaces.shift(table, first, last);
		try {
			while(updatedRecsAsHashtables.size()>0) {
				InsertionCheckers.insertionPositionAndPageNumber(engine, table.getTableName(), updatedRecsAsHashtables.get(0));
				//insertionPositionAndPageNumber updates all indices
				updatedRecsAsHashtables.remove(0);
			}
		}
		catch (DBAppException e1) {
			System.out.println(e1.getMessage());
		}
	}
	
	public static Object parseString(String s, String ckType) {
		Object output = null;
		switch(ckType) {
		case "java.lang.Integer":
			output = Integer.parseInt(s);
			break;
		case "java.lang.String":
			output = s;
			break;
		case "java.lang.Double":
			output = Double.parseDouble(s);
			break;
		case "java.awt.Polygon": 
			String[] points = s.split("\\),\\(");
			//System.out.println(Arrays.toString(points));
			points[0] = points[0].substring(1,points[0].length());
			String[] s2 = points[points.length-1].split("\\)");
			points[points.length-1] = s2[0];
			//System.out.println(Arrays.toString(points));
			Polygon p = new Polygon();
			for(int i=0;i<points.length;i++) {
				String[] coordinates = points[i].split(",");
				int x = Integer.parseInt(coordinates[0]);
				int y = Integer.parseInt(coordinates[1]);
				p.addPoint(x, y);
			}
			output = p;
			break;
		case "java.lang.Boolean":  
			output = Boolean.parseBoolean(s);
			break;
		case "java.util.Date" :
			try {
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
				output = formatter.parse(s);
				break;
			}
			catch(ParseException e) {
				System.out.println("error reading the input clustering key.");
			}
		default:;
		}
		return output;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void updateWithBTIndex(DBApp engine, String strTableName, String strClusteringKey, Hashtable<String,Object> htblColNameValue) throws DBAppException {
		ArrayList<String> inputColumns = new ArrayList<String>(htblColNameValue.keySet());
		Table table = getTable(engine, strTableName);
		BPTree index = Index.loadBTIndex(strTableName, table.getClusteringKey());
		Object key = parseString(strClusteringKey, table.getClusteringKeyType());
		Ref ref = index.search((Comparable) key);
		if(ref == null) {
			throw new DBAppException("There are no records with a clustering key of value " + strClusteringKey);
		}
		if(!inputColumns.contains(getTable(engine, strTableName).getClusteringKey())) {	//clustering key is not updated
			System.out.println("clustering key is not updated");
			try {
				updateValues(table, ref, key, htblColNameValue);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else {
			try {
				updateValuesAndSort(engine, table, ref, key, htblColNameValue);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void updateWithRTIndex(DBApp engine, String strTableName, String strClusteringKey, Hashtable<String,Object> htblColNameValue) throws DBAppException {
		ArrayList<String> inputColumns = new ArrayList<String>(htblColNameValue.keySet());
		Table table = getTable(engine, strTableName);
		RTree index = Index.loadRTIndex(strTableName, table.getClusteringKey());
		Object key = parseString(strClusteringKey, table.getClusteringKeyType());
		RRef ref = index.search((Polygon) key);
		if(ref == null) {
			throw new DBAppException("There are no records with a clustering key of value " + strClusteringKey);
		}
		if(!inputColumns.contains(getTable(engine, strTableName).getClusteringKey())) {	//clustering key is not updated
			System.out.println("clustering key is not updated");
			try {
				updateValues(table, ref, key, htblColNameValue);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else {
			try {
				updateValuesAndSort(engine, table, ref, key, htblColNameValue);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void updateValues(Table table, Reference ref, Object ck, Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException {
		boolean polygon = false;
		if(ck instanceof Polygon)
			polygon = true;	
		Enumeration<String> e = htblColNameValue.keys();
		ArrayList updateIndices = new ArrayList();
		ArrayList<Object> updateValues = new ArrayList<Object>();
		while(e.hasMoreElements()) {
			String colName = e.nextElement();
			int i = table.getColNames().indexOf(colName);
			updateIndices.add(i);
			updateValues.add(htblColNameValue.get(colName));
		}
		if(!(ref instanceof OverflowPage)) {
			if(!(ref instanceof ROverflowPage)) {
				System.out.println("directly update");
				int pIndex = ref.getPage();
				int rIndex = ref.getIndexInPage();
				Page page = Page.loadPage(table.getTableName(), (pIndex+1));
				boolean cont = true;
				if(polygon) {
					if(!ParsePolygon.compareByCoordinates(ck, page.get(rIndex)[table.getClusteringKeyIndex()])) {
						cont= false;
					}
				}
				if(cont) {
					for(int i=0;i<updateIndices.size();i++) {
						int indexToUpdate = (int) updateIndices.get(i);
						Object newValue = updateValues.get(i);
						page.get(rIndex)[indexToUpdate] = newValue;
					}
					page.get(rIndex)[page.get(rIndex).length-1] = new Date();
					page.savePage(table.getTableName(), pIndex+1);
				}
			}
			else {
				System.out.println("using R Tree");
				ROverflowPage overflowPage = (ROverflowPage) ref;
				System.out.println("R overflow page");
				while(overflowPage != null) {
					for(RRef indices : overflowPage.getRecords()) {
						int pIndex = indices.getPage();
						Page page = Page.loadPage(table.getTableName(), pIndex+1);
						int rIndex = indices.getIndexInPage();
						boolean cont = true;
						if(polygon) {
							if(!ParsePolygon.compareByCoordinates(ck, page.get(rIndex)[table.getClusteringKeyIndex()])) {
								cont= false;
							}
						}
						if(cont) {
							for(int i=0;i<updateIndices.size();i++) {
								int indexToUpdate = (int) updateIndices.get(i);
								Object newValue = updateValues.get(i);
								page.get(rIndex)[indexToUpdate] = newValue;
							}
							page.get(rIndex)[page.get(rIndex).length-1] = new Date();
							page.savePage(table.getTableName(), pIndex+1);
						}
					}
					overflowPage = overflowPage.getNext();
				}
			}
		}
		else {
			OverflowPage overflowPage = (OverflowPage) ref;
			System.out.println("B+ overflow page");
			while(overflowPage != null) {
				for(Ref indices : overflowPage.getRecords()) {
					int pIndex = indices.getPage();
					Page page = Page.loadPage(table.getTableName(), pIndex+1);
					int rIndex = indices.getIndexInPage();
					boolean cont = true;
					if(polygon) {
						if(!ParsePolygon.compareByCoordinates(ck, page.get(rIndex)[table.getClusteringKeyIndex()])) {
							cont= false;
						}
					}
					if(cont) {
						for(int i=0;i<updateIndices.size();i++) {
							int indexToUpdate = (int) updateIndices.get(i);
							Object newValue = updateValues.get(i);
							page.get(rIndex)[indexToUpdate] = newValue;
						}
						page.get(rIndex)[page.get(rIndex).length-1] = new Date();
						page.savePage(table.getTableName(), pIndex+1);
					}
				}
				overflowPage = overflowPage.getNext();
			}
		}
		table.saveTable();
		Index.updateAllTableInd(table.getTableName());
	}
	//new
	@SuppressWarnings({ "rawtypes", "unchecked"})
	public static void updateValuesAndSort(DBApp engine, Table table, Reference ref, Object ck, Hashtable<String,Object> htblColNameValue) throws IOException, DBAppException {
		boolean polygon = false;
		if(ck instanceof Polygon)
			polygon = true;
		Enumeration<String> e = htblColNameValue.keys();
		ArrayList<Integer> updateIndices = new ArrayList();
		ArrayList<Object> updateValues = new ArrayList<Object>();
		while(e.hasMoreElements()) {
			String colName = e.nextElement();
			int i = table.getColNames().indexOf(colName);
			updateIndices.add(i);
			updateValues.add(htblColNameValue.get(colName));
		}
		ArrayList<Object[]> updatedRecords = new ArrayList<Object[]>();
		int[] first = new int[2];
		int[] last = new int[2];
		if(!(ref instanceof OverflowPage)) {
			if(!(ref instanceof ROverflowPage)) {
				System.out.println("directly update");
				int pIndex = ref.getPage();
				int rIndex = ref.getIndexInPage();
				first[0] = pIndex; 
				last[0] = pIndex; 
				first[1] = rIndex; 
				last[1]= rIndex;
				Page page = Page.loadPage(table.getTableName(), (pIndex+1));
				boolean cont = true;
				if(polygon) {
					if(!ParsePolygon.compareByCoordinates(ck, page.get(rIndex)[table.getClusteringKeyIndex()])) {
						cont= false;
					}
				}
				if(cont) {
					for(int i=0;i<updateIndices.size();i++) {
						int indexToUpdate = updateIndices.get(i);
						Object newValue = updateValues.get(i);
						page.get(rIndex)[indexToUpdate] = newValue;
					}
					Object[] updatedRecord = page.get(rIndex);
					page.remove(rIndex);
					Object[] updatedRecordWithoutDate = new Object[updatedRecord.length-1];
					for(int i=0;i<updatedRecordWithoutDate.length;i++) {
						updatedRecordWithoutDate[i] = updatedRecord[i];
					}
					updatedRecords.add(updatedRecordWithoutDate);
					page.savePage(table.getTableName(), pIndex+1);
				}
			}
			else {
				ROverflowPage overflowPage = (ROverflowPage) ref;
				System.out.println("R- overflow page");
				first = overflowPage.getMinPageNo();
				while(overflowPage!=null) {
					last = overflowPage.getMaxPageNo();
					overflowPage = overflowPage.getNext();
				}
				int pIndexF = first[0];
				int rIndexF = first[1];
				int pIndexL = last[0];
				int rIndexL = last[1];
				Page page = null;
				while(pIndexF<pIndexL) {
					page = Page.loadPage(table.getTableName(), pIndexF+1);
					while(rIndexF<=page.size()-1) {
						boolean cont = true;
						if(polygon) {
							if(!ParsePolygon.compareByCoordinates(ck, page.get(rIndexF)[table.getClusteringKeyIndex()])) {
								cont= false;
								rIndexF++;
							}
						}
						if(cont) {
							for(int i=0;i<updateIndices.size();i++) {
								int indexToUpdate = updateIndices.get(i);
								Object newValue = updateValues.get(i);
								page.get(rIndexF)[indexToUpdate] = newValue;
							}
							Object[] updatedRecord = page.get(rIndexF);
							page.remove(rIndexF);
							Object[] updatedRecordWithoutDate = new Object[updatedRecord.length-1];
							for(int i=0;i<updatedRecordWithoutDate.length;i++) {
								updatedRecordWithoutDate[i] = updatedRecord[i];
							}
							updatedRecords.add(updatedRecordWithoutDate);
						}
					}
					page.savePage(table.getTableName(), pIndexF+1);
					pIndexF++;
					rIndexF = 0;
				}
				page = Page.loadPage(table.getTableName(), pIndexF+1);
				while(rIndexF<=rIndexL) {
					boolean cont = true;
					if(polygon) {
						if(!ParsePolygon.compareByCoordinates(ck, page.get(rIndexF)[table.getClusteringKeyIndex()])) {
							cont= false;
							rIndexF++;
							System.out.println(rIndexF);
						}
					}
					if(cont) {
						for(int i=0;i<updateIndices.size();i++) {
							int indexToUpdate = (int) updateIndices.get(i);
							Object newValue = updateValues.get(i);
							page.get(rIndexF)[indexToUpdate] = newValue;
						}
						Object[] updatedRecord = page.get(rIndexF);
						page.remove(rIndexF);
						Object[] updatedRecordWithoutDate = new Object[updatedRecord.length-1];
						for(int i=0;i<updatedRecordWithoutDate.length;i++) {
							updatedRecordWithoutDate[i] = updatedRecord[i];
						}
						updatedRecords.add(updatedRecordWithoutDate);
						rIndexL--;
					}
				}
				page.savePage(table.getTableName(), pIndexF+1);
				table.saveTable();
			}
		}
		else {
			OverflowPage overflowPage = (OverflowPage) ref;
			System.out.println("B+ overflow page");
			first = overflowPage.getMinPageNo();
			while(overflowPage!=null) {
				last = overflowPage.getMaxPageNo();
				overflowPage = overflowPage.getNext();
			}
			int pIndexF = first[0];
			int rIndexF = first[1];
			int pIndexL = last[0];
			int rIndexL = last[1];
			Page page = null;
			while(pIndexF<pIndexL) {
				page = Page.loadPage(table.getTableName(), pIndexF+1);
				while(rIndexF<=page.size()-1) {
					boolean cont = true;
					if(polygon) {
						if(!ParsePolygon.compareByCoordinates(ck, page.get(rIndexF)[table.getClusteringKeyIndex()])) {
							cont= false;
							rIndexF++;
						}
					}
					if(cont) {
						for(int i=0;i<updateIndices.size();i++) {
							int indexToUpdate = updateIndices.get(i);
							Object newValue = updateValues.get(i);
							page.get(rIndexF)[indexToUpdate] = newValue;
						}
						Object[] updatedRecord = page.get(rIndexF);
						page.remove(rIndexF);
						Object[] updatedRecordWithoutDate = new Object[updatedRecord.length-1];
						for(int i=0;i<updatedRecordWithoutDate.length;i++) {
							updatedRecordWithoutDate[i] = updatedRecord[i];
						}
						updatedRecords.add(updatedRecordWithoutDate);
					}
				}
				page.savePage(table.getTableName(), pIndexF+1);
				pIndexF++;
				rIndexF = 0;
			}
			page = Page.loadPage(table.getTableName(), pIndexF+1);
			while(rIndexF<=rIndexL) {
				boolean cont = true;
				if(polygon) {
					if(!ParsePolygon.compareByCoordinates(ck, page.get(rIndexF)[table.getClusteringKeyIndex()])) {
						cont= false;
						rIndexF++;
						System.out.println(rIndexF);
					}
				}
				if(cont) {
					for(int i=0;i<updateIndices.size();i++) {
						int indexToUpdate = (int) updateIndices.get(i);
						Object newValue = updateValues.get(i);
						page.get(rIndexF)[indexToUpdate] = newValue;
					}
					Object[] updatedRecord = page.get(rIndexF);
					page.remove(rIndexF);
					Object[] updatedRecordWithoutDate = new Object[updatedRecord.length-1];
					for(int i=0;i<updatedRecordWithoutDate.length;i++) {
						updatedRecordWithoutDate[i] = updatedRecord[i];
					}
					updatedRecords.add(updatedRecordWithoutDate);
					rIndexL--;
				}
			}
			page.savePage(table.getTableName(), pIndexF+1);
			table.saveTable();
		}
		Vector<Hashtable<String,Object>> updatedRecsAsHashtables = new Vector<Hashtable<String,Object>>();
		while(updatedRecords.size()>0) {
			Object[] updatedRecord = updatedRecords.get(0);
			Hashtable<String,Object> record = new Hashtable<String,Object>();
			for(int i =0;i<updatedRecord.length;i++) {
				record.put(table.getColNames().get(i), updatedRecord[i]);
			}
			updatedRecsAsHashtables.add(record);
			updatedRecords.remove(0);
		}
		EmptySpaces.shift(table, first, last);
		Index.updateAllTableInd(table.getTableName());
		try {
			while(updatedRecsAsHashtables.size()>0) {
				InsertionCheckers.insertionPositionAndPageNumber(engine, table.getTableName(), updatedRecsAsHashtables.get(0));	//until we fix insert with index
				//InsertionCheckers.insertingUsingAnIndex(table.getTableName(), updatedRecsAsHashtables.get(0), table.getClusteringKey());
				updatedRecsAsHashtables.remove(0);
			}
		}
		catch (DBAppException | IOException e1) {	//ClassNotFoundException
			System.out.println(e1.getMessage());
		}
	}
}