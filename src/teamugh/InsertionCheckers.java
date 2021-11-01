package teamugh;

import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import bPlusTree.BPTree;
import bPlusTree.BPTreeInnerNode;
import bPlusTree.BPTreeLeafNode;
import bPlusTree.BPTreeNode;
import bPlusTree.OverflowPage;
import bPlusTree.Ref;
import rTree.ROverflowPage;
import rTree.RRef;
import rTree.RTree;
import rTree.RTreeInnerNode;
import rTree.RTreeLeafNode;
import rTree.RTreeNode;

public class InsertionCheckers {

	public static boolean tableNotExists(DBApp engine, String strTableName) {
		return CreationCheckers.tableNotExists(engine, strTableName);
	}
//	this is the same as hasIndex() in class Index
//	the one in class Index handles TouchDate and this one doesn't
//	@SuppressWarnings("resource")
//	public static boolean colHasIndex(String strColName, String strTableName) {
//		try {
//			String line = null;			
//			BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
//
//			while ((line = br.readLine()) != null) {		
//				String[] content = line.split(",");
//				if(strTableName.equals(content[0]) && strColName.equals(content[1]) && content[4].equalsIgnoreCase("true")) {	
//					return true;
//				}
//			}
//			br.close();
//		} 
//		catch (IOException e) {
//			System.out.println("Failed to check metadata file.");
//		}
//		return false;
//	}
//
//		public static String getClusteringKeyColName(String strTableName) {
//			
//		}

	public static boolean checkColumnNames(DBApp engine, String strTableName, Hashtable<String,Object> htblColNameValue) {
		Table x = null;
		for(int index = 0; index < engine.tables.size(); index++) {
			x = engine.tables.get(index);
			if(x.getTableName().equals(strTableName)) {
				break;	
			}
		}
		ArrayList<String> columnNames = new ArrayList<String> (htblColNameValue.keySet());
		if(columnNames.size() != x.getColNames().size()-1)
			return false;
		else {
			for(int i = 0 ; i < columnNames.size(); i++) {
				if(!(columnNames.get(i).equals(x.getColNames().get(i))))
					return false;
			}	
			return true;
		}
	}

	public static void insertionPositionAndPageNumber(DBApp engine, String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException {
		Object[] m = new Object[htblColNameValue.size()+1];	//to hold the values to be inserted
		Enumeration<String> y = htblColNameValue.keys();	//the column names
		Table b = UpdateCheckers.getTable(engine, strTableName);
		while(y.hasMoreElements()) {
			String z = y.nextElement();
			int c = b.getColNames().indexOf(z);
			m[c] = htblColNameValue.get(z);
		}
		m[m.length-1] = new Date();
		int x = b.getNoOfPages();
		Object[] ckPosition = {b.getClusteringKeyIndex(), b.getClusteringKeyType()}; //clusteringKeyPosition(strTableName);	
		if(ckPosition[0]==null) {		//because there might not be a defined clustering key according to clusteringKeyPosition method??
			throw new DBAppException("No clustering key is defined for this table.");
		}
		if (x==0) {		//create a new page 
			//System.out.println("new page");
			Page p = new Page();
			b.addPage();		//increment the no of pages
			p.add(0, m);
			p.savePage(strTableName, 1);		//serialize the page
			b.saveTable();
			Index.updateAllTableInd(strTableName);
			return;
		}
		else {	//x!=0
			int g = pageToInsertInto(b.getTableName(), m, 0, b.getNoOfPages()-1, (int) ckPosition[0], (String) ckPosition[1]);	//binary search to find the page we should insert into
			if(g==-1) {
				g=0;
			}
			Page p = Page.loadPage(strTableName, (g+1));
			//System.out.println("G " + g);//load the page
			if(p.size() == p.getMaxRows() && g==b.getNoOfPages()-1) { //check whether the page is full ++ check whether it's the last page
				//System.out.println("last page");
				Page p1 = new Page();
				b.addPage();
				Object[] lastElementinPage = p.get(p.size() - 1);		//the last tuple in the page and the one with the maximum value of the clustering key
				int checker = helperMaxShiftToNewPage(lastElementinPage, m, strTableName);			
				if(checker == -1) {			//insert in the same page and shift max to a new page
					//System.out.println("insert in the same page and shift the maximum");
					insertion(p, m, 0, p.size()-1, (int) ckPosition[0], ckPosition[1]);
					p.remove(lastElementinPage);
					p.savePage(strTableName, (g+1));
					p1.add(0, lastElementinPage);
					p1.savePage(strTableName, (g+2));
					b.saveTable();
					Index.updateAllTableInd(strTableName);
					return;
				}
				if(checker == 1) {			//insert the record in the new page
					//System.out.println("insert in a new page");
					p1.add(0,m);
					p1.savePage(strTableName, (g+2));
					b.saveTable();
					Index.updateAllTableInd(strTableName);
					return;
				}
			}
			else {
				if(p.size() == p.getMaxRows() && g<b.getNoOfPages()-1) {
					//System.out.println("page is full and it is not the last page");
					Object[] lastElementinPage = p.get(p.size()-1);
					int checker = helperMaxShiftToNewPage(lastElementinPage, m, strTableName);
					if(checker == -1) {
						//System.out.println("insert in the same page and shift max to the next page AND keep shifting until we reach the last page");
						boolean needNewPageAfterShifting = false;
						if((Page.loadPage(strTableName, b.getNoOfPages())).size() == p.getMaxRows()) {	//the last page is full
							//System.out.println("we need a new page after we shift");
							needNewPageAfterShifting = true;
						}
						insertion(p, m, 0, p.size()-1, (int) ckPosition[0], ckPosition[1]);
						p.savePage(strTableName, (g+1));
						int index = g+1;
						int prev = g;
						//shift first
						while(index < b.getNoOfPages()) {
							//System.out.println("we are inside the loop " + "index: " + index + " prev: " + prev + " b.getPages.size() = " + b.getNoOfPages());
							Page pPrev = Page.loadPage(strTableName, prev+1);
							pPrev.remove(pPrev.size()-1);
							pPrev.savePage(strTableName, prev+1);
							Page pNext= Page.loadPage(strTableName, index+1);
							Object[] lastElementinNextPage = pNext.get(pNext.size()-1);
							pNext.add(0, lastElementinPage);
							pNext.savePage(strTableName, index+1);
							lastElementinPage = lastElementinNextPage;
							index++;
							if(index==b.getNoOfPages() &&  pNext.size() > pNext.getMaxRows()) {	//we bypassed the last page and it was previously full
								pNext.remove(pNext.size()-1);
								pNext.savePage(strTableName, index);
							}
							prev++;
						}
						//then check whether we need a new page for the last element that was in the last page before the new insertion
						if(needNewPageAfterShifting) {
							Page p1 = new Page();
							b.addPage();
							p1.add(0, lastElementinPage);
							p1.savePage(strTableName, b.getNoOfPages());
						}
						b.saveTable();
						Index.updateAllTableInd(strTableName);
						return;
					}
					if(checker == 1) {
						//System.out.println("insert the record on top of the next page and keep shifting");
						boolean needNewPageAfterShifting = false;
						if((Page.loadPage(strTableName, b.getNoOfPages())).size() ==  p.getMaxRows()) { //the last page is full
							needNewPageAfterShifting = true;
						}
						int index = g+1;
						p = Page.loadPage(strTableName, index+1);
						p.add(0, m);
						lastElementinPage = p.get(p.size()-1);
						p.savePage(strTableName, index+1);
						int prev = index;
						index++;
						if(index==b.getNoOfPages() && needNewPageAfterShifting) {
							p.remove(lastElementinPage);
							p.savePage(strTableName, index);
						}
						else {
							while(index<b.getNoOfPages()) {
								Page pPrev = Page.loadPage(strTableName, prev+1);
								pPrev.remove(pPrev.size()-1);
								pPrev.savePage(strTableName, prev+1);
								Page pNext= Page.loadPage(strTableName, index+1);
								Object[] lastElementinNextPage = pNext.get(pNext.size()-1);
								pNext.add(0, lastElementinPage);
								pNext.savePage(strTableName, index+1);
								lastElementinPage = lastElementinNextPage;
								index++;
								if(index==b.getNoOfPages() && pNext.size() > pNext.getMaxRows()) {
									pNext.remove(pNext.size()-1);
									pNext.savePage(strTableName, index);
								}
								prev++;
							}
						}
						if(needNewPageAfterShifting) {
							Page p1 = new Page();
							b.addPage();
							p1.add(0, lastElementinPage);
							p1.savePage(strTableName, b.getNoOfPages());
						}
						b.saveTable();
						Index.updateAllTableInd(strTableName);
						return;
					}
				}
				else {
					if(p.size() < p.getMaxRows())	{	//the page is not full; if it's not full, it's the last page
						//System.out.println("page not full yet");
						insertion(p, m, 0, p.size()-1, (int) ckPosition[0], ckPosition[1]);
						p.savePage(strTableName, (g+1));
						b.saveTable();
						Index.updateAllTableInd(strTableName);
						return;
					}
				}
			}
		}
	}

	public static int helperMaxShiftToNewPage (Object[] max, Object[] x, String strTableName) throws IOException {
		Object [] temp = clusteringKeyPosition(strTableName);
		int ck = (int) temp[0];
		String type = (String) temp[1];
		int compResult = -1;

		switch(type) {
		case "java.lang.Integer":
			Integer i1 =((Integer) x[ck]);
			Integer i2 = (Integer) max[ck];
			compResult = i1.compareTo(i2);
			break;
		case "java.lang.String":
			String s1 =((String) x[ck]);
			String s2 = (String) max[ck];
			compResult = s1.compareTo(s2);
			break;
		case "java.lang.Double":
			Double d1 =((Double) x[ck]);
			Double d2 = (Double) max[ck];
			compResult = d1.compareTo(d2);
			break;
		case "java.awt.Polygon": 
			Polygon p1 = (Polygon) x[ck];
			Polygon p2 = (Polygon) max[ck];
			Double d = (double) (p1.getBounds().getSize().width * p1.getBounds().getSize().height);
			Double c = (double) (p2.getBounds().getSize().width * p2.getBounds().getSize().height);
			compResult = d.compareTo(c);
			break;
		case "java.lang.Boolean":  
			Boolean b1 =((Boolean) x[ck]);
			Boolean b2 = (Boolean) max[ck];
			compResult = b1.compareTo(b2);
			break;
		case "java.util.Date":  
			Integer d3 =((Integer) x[ck]);
			Integer d4 = (Integer) max[ck];
			compResult = d3.compareTo(d4);
			break;
		default:;
		}
		if(compResult >= 0) {
			return 1;	//greater than max or equal to it; insert the record in a new page
		}
		if(compResult < 0) 
			return -1;	//less than the max so insert in the same page and shift max to a new page
		return 0;
	}

	@SuppressWarnings("resource")
	public static Object [] clusteringKeyPosition(String strTableName) {
		try {	
			String line = null;
			int i = 0;
			BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
			while ((line = br.readLine()) != null) {		//was while(line!=null) which is an infinite loop
				String[] content = line.split(",");
				if (content[0].equals(strTableName)) {
					if (content[3].equalsIgnoreCase("true")) {		//was .equals(true) and true is not a String
						return new Object[] {i, content[2]} ;		//returns the position of the clustering key and its column name
					}
					i++;
				}
			}
			br.close();
		}
		catch (IOException e) {
			e.printStackTrace();		
		}
		return new Object [] {null};		//means that there is no clustering key?
	}

	@SuppressWarnings("resource")
	public static boolean checkKey(Hashtable<String,Object> htblColNameValue, String strTableName) {	
		ArrayList<String> datatypes = new ArrayList<String> ();	//to hold the data types of the input
		Enumeration<String> e = htblColNameValue.keys();
		while(e.hasMoreElements()) {
			String key = e.nextElement();
			datatypes.add((htblColNameValue.get(key)).getClass().getName());
		}
		try {
			BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
			String line;
			int i=0;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");
				if (values[0].equals(strTableName)) {
					if(!values[2].equals(datatypes.get(i))) {		//compare the data type in the meta data with that of the corresponding input
						return false;
					}	
					i++;
				}
			}
			br.close();	//was missing
			return true;
		}
		catch (Exception ex) {
			return false;
		}		
	}

	public static void insertion(Vector<Object []> vec, Object[] z, int l, int r, int ck, Object type) {	//the vector to insert into, the tuple to be inserted, lower limit, upper limit, clustering key position, data type of the clustering key
		int u = insertionIntoVector.helper(vec, z, l, r,ck, (String) type);
		vec.add(u, z);
	}

	public static int pageToInsertInto(String tableName, Object[] input, int l, int r, int ckIndex, String type) {
		if (r==l) {
			//System.out.println("page index to insert into: " + r);
			return r;
		}
		if (r>l) {
			int midPageIndex = l + (r - l)/2;
			Page current = Page.loadPage(tableName, (midPageIndex+1));
			Object[] min = current.get(0);
			Object[] max = current.get(current.size()-1);

			if(compareInput(input[ckIndex],min[ckIndex], type) >= 0 && compareInput(input[ckIndex],max[ckIndex], type) <= 0) {
				//System.out.println(input[ckIndex] + " more than " + min[ckIndex] + " and " + input[ckIndex] + " less than " + max[ckIndex] + " ; page index to insert into: " + midPageIndex);
				return midPageIndex;
			}
			if(compareInput(input[ckIndex],min[ckIndex], type) < 0) {	//input is less than the minimum
				//System.out.println(input[ckIndex] + " less than " + min[ckIndex]);
				return pageToInsertInto(tableName, input, l, midPageIndex-1, ckIndex, type);
			}
			if(compareInput(input[ckIndex],max[ckIndex], type) > 0) {	//input is more than the maximum
				//System.out.println(input[ckIndex] + " more than " + max[ckIndex]);
				return pageToInsertInto(tableName, input, midPageIndex+1, r, ckIndex, type);
			}
		}
		return -1;
	}

	public static int compareInput(Object i, Object m, String type) {
		int compResult = -1;
		switch(type) {
		case "java.lang.Integer":
			Integer i1 = (Integer) i;
			Integer i2 = (Integer) m;
			compResult = i1.compareTo(i2);
			break;
		case "java.lang.String":
			String s1 = (String) i;
			String s2 = (String) m;
			compResult = s1.compareTo(s2);
			break;
		case "java.lang.Double":
			Double d1 = (Double) i;
			Double d2 = (Double) m;
			compResult = d1.compareTo(d2);
			break;
		case "java.awt.Polygon": 
			Polygon p1 = (Polygon) i;
			Polygon p2 = (Polygon) m;
			Double d = (double) (p1.getBounds().getSize().width * p1.getBounds().getSize().height);
			Double c = (double) (p2.getBounds().getSize().width * p2.getBounds().getSize().height);
			compResult = d.compareTo(c);
			break;
		case "java.lang.Boolean":  
			Boolean b1 = (Boolean) i;
			Boolean b2 = (Boolean) m;
			compResult = b1.compareTo(b2);
			break;
		case "java.util.Date" :  
			Integer d3 = (Integer) i;
			Integer d4 = (Integer) m;
			compResult = d3.compareTo(d4);
			break;
		default:;
		}
		if (compResult == 0) {		//input is equal to the existing element
			return 0;
		}
		if (compResult > 0) {		//input is more than the existing element
			return 1;
		}
		return -1;		//input is less than the existing element
	}

	@SuppressWarnings({"rawtypes", "unchecked", "resource"})
	public static void insertingUsingAnIndex(String strTableName, Hashtable<String,Object> htblColNameValue, String indexCol) throws DBAppException, IOException, ClassNotFoundException {
		//load the table
		FileInputStream fi = new FileInputStream("data/" + strTableName + ".ser");
		ObjectInputStream oi = new ObjectInputStream(fi);
		Table b = (Table) oi.readObject();

		//put the column to be inserted in an object[]
		Object[] values = new Object[htblColNameValue.size()+1];	//to hold the values to be inserted
		Enumeration<String> y = htblColNameValue.keys();	//the column names
		while(y.hasMoreElements()) {
			String z = y.nextElement();
			int c = b.getColNames().indexOf(z);
			values[c] = htblColNameValue.get(z);
		}
		values[values.length-1] = new Date();	//TouchDate
			
		//get the clusteringKey Position
		Object[] clusPos = new Object[] {b.getClusteringKeyIndex(), b.getClusteringKeyType()};
		if(clusPos[0] == null) {
			throw new DBAppException("The table you're trying to insert into does not have a defined clustering key");
		}
		int x =  (int) clusPos[0];
		
		//page & index insertion positions to be filled
		int insertionPage = -1;
		int insertionIndex = -1;
		//indicates loop break
		boolean posiFound = false;
		//indicates whether the wanted child node is found or not
		boolean childFound = false;	
				
		
		int i = 0;
				
		if(b.getNoOfPages() == 0) {
			insertionIndex = 0;
			insertionPage = 1;
			Page page = new Page();
			b.addPage();
			page.add(0, values);
			page.savePage(strTableName, insertionPage);
			b.saveTable();
			Index.updateAllTableInd(strTableName);
			return;
		}
		
		if(Index.whichTreeToUse(strTableName, indexCol).equalsIgnoreCase("bplustree")) {
	
			//get the clusteringKey Value
			Comparable clusKey = (Comparable) values[x];
			if(clusKey instanceof String) {
				Object ck = UpdateCheckers.parseString((String) clusKey, (String) clusPos[1]);
				clusKey = (Comparable) ck;
			}
			//load the index
			FileInputStream fis = new FileInputStream("data/" + strTableName + "." + indexCol + ".index.class");
			ObjectInputStream ois = new ObjectInputStream(fis);		
			BPTree bTree = (BPTree) ois.readObject();
			fis.close();
			ois.close();
			fi.close();
			oi.close();	
			
			//load the root
			BPTreeNode node = bTree.root();
		
			while(!posiFound) {
				childFound = false;				
				if(node instanceof BPTreeLeafNode) {
					node = (BPTreeLeafNode) node;				
					for(i = 0; i < node.getNumberOfKeys() && !posiFound; i++) {
						Comparable temp = node.getKey(i);
						if(temp.compareTo(clusKey) == 0) {
	
							Ref ref = ((BPTreeLeafNode) node).getRecord(i);
							if(ref instanceof OverflowPage) {
								OverflowPage op = (OverflowPage) ref;
								while(op.getNext()!=null) {
									op = op.getNext();
								}
								Ref last = op.getRecords(op.getRecords().size()-1);
								insertionPage = last.getPage()+1;
								insertionIndex =last.getIndexInPage();
								posiFound = true;
								break;
							}
							else {
								insertionPage = ref.getPage()+1;
								insertionIndex = ref.getIndexInPage();
								posiFound = true;
								break;
							}
						}
	
						if(temp.compareTo(clusKey) < 0) {
							System.out.println("leaf loop of <");
							//load next key
						}
	
						if(temp.compareTo(clusKey) > 0) {
							System.out.println("leaf loop of > ");
							Ref ref = ((BPTreeLeafNode) node).getRecord(i);
							if(ref instanceof OverflowPage) {
								OverflowPage op = (OverflowPage) ref;
								int[] min = op.getMinPageNo();
								insertionPage = min[0]+1;
								insertionIndex = min[1];
								posiFound = true;
								break;
							}
							else {
								insertionPage = ref.getPage()+1;
								insertionIndex = ref.getIndexInPage();
								posiFound = true;
								break;
							}
						}	
					}
					if(!posiFound) {
						System.out.println("hello");
						Ref ref = ((BPTreeLeafNode) node).getRecord(i-1);
						if(ref instanceof OverflowPage) {
							OverflowPage op = (OverflowPage) ref;
							while(op.getNext()!=null) {
								op = op.getNext();
							}
							int[] max = op.getMaxPageNo();
							insertionPage = max[0]+1;
							insertionIndex = max[1]+1;
							posiFound = true;
						}
						else {
							insertionPage = ref.getPage()+1;
							insertionIndex = ref.getIndexInPage() + 1;
							posiFound = true;
						}
					}	
				}
				else {
					if(node instanceof BPTreeInnerNode) {
						for(i = 0; i < node.getNumberOfKeys() && !childFound; i++) {
							Comparable temp = node.getKey(i);
	
							if(temp.compareTo(clusKey) == 0) {
								node = ((BPTreeInnerNode) node).getChild(i+1);
								childFound = true;
								break;
							}
	
							if(temp.compareTo(clusKey) < 0) {
	
							}
	
							if(temp.compareTo(clusKey) > 0) {
								node = ((BPTreeInnerNode) node).getChild(i);
								childFound = true;
								break;
							}
						}
	
						if(!childFound) {
							node = ((BPTreeInnerNode) node).getChild(i);
							childFound = true;
						}
					}
				}
			}
			insertIntoTableUsingAnIndex(strTableName, values, indexCol, insertionPage, insertionIndex, b, clusKey);
		}
		else if(Index.whichTreeToUse(strTableName, indexCol).equalsIgnoreCase("rtree")) {
			//get the clusteringKey Value
			Polygon clusKey = (Polygon) values[x];
			//load the index
			FileInputStream fis = new FileInputStream("data/" + strTableName + "." + indexCol + ".index.class");
			ObjectInputStream ois = new ObjectInputStream(fis);		
			RTree rTree = (RTree) ois.readObject();
			fis.close();
			ois.close();
			fi.close();
			oi.close();	
			
			//load the root
			RTreeNode node = rTree.root();
		
			while(!posiFound) {
				childFound = false;				
				if(node instanceof RTreeLeafNode) {
					node = (RTreeLeafNode) node;				
					for(i = 0; i < node.getNumberOfKeys() && !posiFound; i++) {
						Polygon temp = node.getKey(i);
						if(ParsePolygon.compareByArea(temp,clusKey) == 0) {
	
							RRef ref = ((RTreeLeafNode) node).getRecord(i);
							if(ref instanceof ROverflowPage) {
								ROverflowPage op = (ROverflowPage) ref;
								while(op.getNext()!=null) {
									op = op.getNext();
								}
								RRef last = op.getRecords(op.getRecords().size()-1);
								insertionPage = last.getPage()+1;
								insertionIndex =last.getIndexInPage();
								posiFound = true;
								break;
							}
							else {
								insertionPage = ref.getPage()+1;
								insertionIndex = ref.getIndexInPage();
								posiFound = true;
								break;
							}
						}
	
						if(ParsePolygon.compareByArea(temp,clusKey) < 0) {
							System.out.println("leaf loop of <");
							//load next key
						}
	
						if(ParsePolygon.compareByArea(temp,clusKey) > 0) {
							System.out.println("leaf loop of > ");
							RRef ref = ((RTreeLeafNode) node).getRecord(i);
							if(ref instanceof ROverflowPage) {
								ROverflowPage op = (ROverflowPage) ref;
								int[] min = op.getMinPageNo();
								insertionPage = min[0]+1;
								insertionIndex = min[1];
								posiFound = true;
								break;
							}
							else {
								insertionPage = ref.getPage()+1;
								insertionIndex = ref.getIndexInPage();
								posiFound = true;
								break;
							}
						}	
					}
					if(!posiFound) {
						System.out.println("hello");
						RRef ref = ((RTreeLeafNode) node).getRecord(i-1);
						if(ref instanceof ROverflowPage) {
							ROverflowPage op = (ROverflowPage) ref;
							while(op.getNext()!=null) {
								op = op.getNext();
							}
							int[] max = op.getMaxPageNo();
							insertionPage = max[0]+1;
							insertionIndex = max[1]+1;
							posiFound = true;
						}
						else {
							insertionPage = ref.getPage()+1;
							insertionIndex = ref.getIndexInPage() + 1;
							posiFound = true;
						}
					}	
				}
				else {
					if(node instanceof RTreeInnerNode) {
						for(i = 0; i < node.getNumberOfKeys() && !childFound; i++) {
							Polygon temp = node.getKey(i);
	
							if(ParsePolygon.compareByArea(temp,clusKey) == 0) {
								node = ((RTreeInnerNode) node).getChild(i+1);
								childFound = true;
								break;
							}
	
							if(ParsePolygon.compareByArea(temp,clusKey) < 0) {
	
							}
	
							if(ParsePolygon.compareByArea(temp,clusKey) > 0) {
								node = ((RTreeInnerNode) node).getChild(i);
								childFound = true;
								break;
							}
						}
	
						if(!childFound) {
							node = ((RTreeInnerNode) node).getChild(i);
							childFound = true;
						}
					}
				}
			}
			insertIntoTableUsingAnIndex(strTableName, values, indexCol, insertionPage, insertionIndex, b, clusKey);
		}		
		Index.updateAllTableInd(strTableName);
		//System.out.println(bTree.toString());
	}

	public static void insertIntoTableUsingAnIndex(String strTableName, Object[] values, String indexCol, int insertionPage, 
			int insertionIndex, Table b,  Object clusKey) throws IOException {
		
		Page p = Page.loadPage(strTableName, insertionPage);
		int maxRows = p.getMaxRows();
		int rowsFull = p.size();
		int noOfPagesInTable = b.getNoOfPages();		

		//inserting when page is not full
		if(maxRows > rowsFull) {
			if(insertionIndex == 0 && insertionPage != 1) {
				System.out.println("IN");
				System.out.println(insertionPage);
				Page pPage = Page.loadPage(strTableName, (insertionPage-1));
				if(pPage.getMaxRows() > pPage.size()) {
					pPage.add(pPage.size(), values);
					pPage.savePage(strTableName,(insertionPage-1));
				}
				else {
					p.add(0, values);
					p.savePage(strTableName,insertionPage);
				}
				b.saveTable();
			}
			else {
				p.add(insertionIndex, values);
				p.savePage(strTableName,insertionPage);
				b.saveTable();
			}
		}		
		//inserting when page is full
		else if(maxRows == rowsFull) {
			//inserting in the last page	
			if(insertionPage == noOfPagesInTable) {		
				Object[] lastElementinPage = p.get(rowsFull-1);
				Page newPage = new Page();	
				b.addPage();
				int check = helperMaxShiftToNewPage(lastElementinPage, values, strTableName);			

				if(check == -1) {	
					p.remove(rowsFull-1);
					p.add(insertionIndex, values);		
					p.savePage(strTableName, insertionPage);			
					newPage.add(0, lastElementinPage);
					newPage.savePage(strTableName, insertionPage + 1);
					b.saveTable();	
				}
				else {
					if(check == 1) {
						newPage.add(0, values);
						newPage.savePage(strTableName, insertionPage + 1);
						b.saveTable();
					}
				}
			}
			//inserting in a middle page
			else if(insertionPage < noOfPagesInTable) { 
				boolean insertionOver = false;
				Object[] lastElementinPage = p.get(rowsFull-1);;																				
				int checker = helperMaxShiftToNewPage(lastElementinPage, values, strTableName);
				if(checker == -1) { 
					p.remove(rowsFull-1);
					p.add(insertionIndex, values);
					p.savePage(strTableName, insertionPage);
					b.saveTable();
					while(!insertionOver) {		
						insertionPage = insertionPage + 1;
						Page newPage = Page.loadPage(strTableName, insertionPage); //loads next page
						Object[] inserting = new Object[lastElementinPage.length];
						for(int j = 0; j < inserting.length; j++) {
							inserting[j] = lastElementinPage[j];
						}
						if(newPage.size() == newPage.getMaxRows()) {	//page is full
							lastElementinPage = newPage.get(newPage.getRows().size()-1);
							if(b.getNoOfPages() == insertionPage) { 	//last page
								newPage.add(0, inserting);
								newPage.remove(newPage.size()-1);
								newPage.savePage(strTableName, insertionPage);
								Page extraPage = new Page();
								b.addPage();
								extraPage.add(0, lastElementinPage);
								extraPage.savePage(strTableName, (insertionPage + 1));
								insertionOver = true;
								b.saveTable();	
							}
							else {	//not last page
								newPage.add(0, inserting);
								newPage.remove(newPage.size()-1);
								newPage.savePage(strTableName, insertionPage);
								b.saveTable();
							}
						}
						else {	//page is not full
							newPage.add(0, inserting);
							newPage.savePage(strTableName, insertionPage);
							b.saveTable();	
							insertionOver = true;
						}
					}
				}
				if(checker == 1) {
					while(!insertionOver) {
						//check if this is now the last page
						//check if it is full or not							
						insertionPage = insertionPage + 1;
						Page newPage = Page.loadPage(strTableName, insertionPage); //loads next page
						Object[] inserting = new Object[values.length];
						for(int j = 0; j < inserting.length; j++) {
							inserting[j] = values[j];
						}
						if(newPage.size() == newPage.getMaxRows()) {
							lastElementinPage = newPage.get(newPage.getRows().size()-1);
							if(b.getNoOfPages() == insertionPage) {
								newPage.remove(newPage.size()-1);
								newPage.add(0, inserting);
								Page extraPage = new Page();
								b.addPage();
								extraPage.add(0, lastElementinPage);
								insertionOver = true;
								newPage.savePage(strTableName, insertionPage);
								b.saveTable();	
							}
							else {
								newPage.remove(lastElementinPage);
								newPage.add(0, inserting);
								newPage.savePage(strTableName, insertionPage);
								b.saveTable();	
							}
						}
						else {
							newPage.add(0, inserting);
							newPage.savePage(strTableName, insertionPage);
							b.saveTable();	
							insertionOver = true;
						}
					}
				}
				b.saveTable();

			}
		}	
	}
	


	
}