package teamugh;

import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;

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

@SuppressWarnings ({"unused", "rawtypes", "unchecked"})
public class SelectStuff {

	public SelectStuff() {
		// TODO Auto-generated constructor stub
	}	

	@SuppressWarnings("resource")
	public static boolean colHasIndex(String strColName, String strTableName) {
		try {
			String line = null;			
			BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
			while ((line = br.readLine()) != null) {		
				String[] content = line.split(",");
				if(strTableName.equals(content[0]) && strColName.equals(content[1]) && content[4].equalsIgnoreCase("true")) {	//check this condition
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
	public static int findIndex(String _strTableName, String _strColName) throws DBAppException {
		int result = -1;
		try {
			String line = null;			
			BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
			int ind = 0;
			while ((line = br.readLine()) != null) {		
				String[] content = line.split(",");
				if(_strTableName.equals(content[0])) { 
						if(_strColName.equals(content[1]))
						result = ind;
						ind++;

				}
			}
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	
	
	public static ArrayList<Object[]> strOperatorType (DBApp engine, String _strTableName,
			String _strColumnName, String _strOperator, Object _objValue) throws DBAppException, IOException { 
		String op = _strOperator;
		ArrayList<Object[]> result = new ArrayList<Object[]>();
		switch(op) {
		case "=":  
			result = Equal (engine, _strTableName, _strColumnName, _objValue); break;
		case "!=": 
			result = NotEqual (engine, _strTableName, _strColumnName, _objValue); break;
		case ">": 
			result = GreaterThan (engine, _strTableName, _strColumnName, _objValue); break;
		case ">=": 
			result = GreaterThanOrEqual (engine, _strTableName, _strColumnName, _objValue); break;
		case "<": 
			result = LessThan (engine, _strTableName, _strColumnName, _objValue); break;
		case "<=": 
			result = LessThanOrEqual (engine, _strTableName, _strColumnName, _objValue); break;
		default:
			throw new DBAppException("Enter a valid operator. Only the =, !=, >, >=, < , <= operators are supported!");
		}
		return result;
	}

	public static ArrayList<Object[]> indexedStrOperatorType (DBApp engine, String _strTableName,
			String _strColumnName, String _strOperator, Object _objValue) throws DBAppException, IOException { 
		String op = _strOperator;
		ArrayList<Object[]> result = new ArrayList<Object[]>();
		switch(op) {
		case "=": 
			result = indexedEqual (engine, _strTableName, _strColumnName, _objValue); break;
		case "!=": 
			System.out.println("!=");
			result = indexedNotEqual (engine, _strTableName, _strColumnName, _objValue); break;
		case ">": 
			result = indexedGreaterThan (engine, _strTableName, _strColumnName, _objValue); break;
		case ">=": 
			result = indexedGreaterThanOrEqual (engine, _strTableName, _strColumnName, _objValue); break;
		case "<": 
			result = indexedLessThan (engine, _strTableName, _strColumnName, _objValue); break;
		case "<=": 
			result = indexedLessThanOrEqual (engine, _strTableName, _strColumnName, _objValue); break;
		default:
			throw new DBAppException("Enter a valid operator. Only the =, !=, >, >=, < , <= operators are supported!");
			
		}
		return result;
	}

	public static ArrayList<Object[]> strarrOperatorType (DBApp engine, 
			String[] _strOperator, ArrayList<ArrayList<Object[]>> x) 
					throws DBAppException, IOException { 

		if(x.size() != _strOperator.length+1) {
			throw new DBAppException("the number of operators does not match the result set! Try again!");
			}

		ArrayList<Object[]> y = x.get(0);
		ArrayList<Object[]> z = x.get(1);
		for(int i = 1; i < x.size(); i++) {	
			String op = _strOperator[i-1].toLowerCase();
			switch(op) {
			case "or": 
				y = OR(y, z);
				break;
			case "xor": 
				y = XOR(y, z);
				break;
			case "and": 
				y = AND(y, z); 
				break;
			}

			if(i < x.size()-1)
				z = x.get(i+1);
		}
		return y;
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
				if(content[0].equalsIgnoreCase(strTableName)) {
					if(c == x) {
						return content[2];
					}
					c++;
				}
			}	
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return type;
	}

	public static ArrayList<Object[]> GreaterThan (DBApp engine, String _strTableName, 
			String _strColumnName, Object _objValue) throws DBAppException, IOException {
		ArrayList<Object[]> result = new ArrayList<Object[]>();
		Table table = UpdateCheckers.getTable(engine, _strTableName);
		int index = findIndex(_strTableName, _strColumnName);
		int compResult;
		String indexType = indexType(index, _strTableName);
		int pSize = table.getNoOfPages();

		for(int i = 0; i < pSize; i++) {	//loop each page
			Page p = Page.loadPage(_strTableName, i+1);	//get the page

			for(int j = 0; j < p.size(); j++) {
				Object[] array = (Object[]) p.get(j);

				switch(indexType) { 
				case "java.lang.Integer":
					Integer i1 =((Integer) _objValue);
					Integer i2 = (Integer) array[index];
					compResult = i1.compareTo(i2);
					break;
				case "java.lang.String":
					String s1 =((String) _objValue);
					String s2 = (String) array[index]; 
					compResult = s1.compareTo(s2);
					break;
				case "java.lang.Double":
					Double d1 =((Double) _objValue);
					Double d2 = (Double) array[index];
					compResult = d1.compareTo(d2);
					break;
				case "java.awt.Polygon": 
					Polygon p1 = ParsePolygon.returnPolygon((String) _objValue);
					Polygon p2 = (Polygon) array[index];
					compResult = ParsePolygon.compareByArea(p1, p2);
					break;
				case "java.lang.Boolean":  
					Boolean b1 =((Boolean) _objValue);
					Boolean b2 = (Boolean) array[index];
					compResult = b1.compareTo(b2);
					break;
				case "java.util.Date" :  
					Integer d3 =((Integer) _objValue);
					Integer d4 = (Integer) array[index];
					compResult = d3.compareTo(d4);
					break;
				default:
					throw new DBAppException();
				}	
				if(compResult < 0 ) {
					String[] temp = checkPolygon(_strTableName);
					if(temp[1].equalsIgnoreCase("true")) {

						int c =Integer.parseInt(temp[0]);
						String pol = ParsePolygon.toString(array[c]);
						System.out.println("PL "+ pol);
						array[c] = pol;
					}
					result.add(array);
				}
			}
		}
		return result;
	}

	public static ArrayList<Object[]> GreaterThanOrEqual(DBApp engine, String _strTableName, 
			String _strColumnName, Object _objValue) throws IOException, DBAppException {
		ArrayList<Object[]> result = new ArrayList<Object[]>();
		Table table = UpdateCheckers.getTable(engine, _strTableName);
		int index = findIndex(_strTableName, _strColumnName);
		
		int compResult;
		String indexType = indexType(index, _strTableName);
		int pSize = table.getNoOfPages();

		for(int i = 0; i < pSize; i++) {	//loop each page
			Page p = Page.loadPage(_strTableName, i+1);	//get the page

			for(int j = 0; j < p.size(); j++) {
				Object[] array = (Object[]) p.get(j);

				switch(indexType) { 
				case "java.lang.Integer":
					Integer i1 =((Integer) _objValue);
					Integer i2 = (Integer) array[index];
					compResult = i1.compareTo(i2);
					break;
				case "java.lang.String":
					String s1 =((String) _objValue);
					String s2 = (String) array[index]; 
					compResult = s1.compareTo(s2);
					break;
				case "java.lang.Double":
					Double d1 =((Double) _objValue);
					Double d2 = (Double) array[index];
					compResult = d1.compareTo(d2);
					break;
				case "java.awt.Polygon": 
					Polygon p1 = ParsePolygon.returnPolygon((String) _objValue);
					Polygon p2 = (Polygon) array[index];
					compResult = ParsePolygon.compareByArea(p1, p2);
					break;
				case "java.lang.Boolean":  
					Boolean b1 =((Boolean) _objValue);
					Boolean b2 = (Boolean) array[index];
					compResult = b1.compareTo(b2);
					break;
				case "java.util.Date" :  
					Integer d3 =((Integer) _objValue);
					Integer d4 = (Integer) array[index];
					compResult = d3.compareTo(d4);
					break;
				default:
					throw new DBAppException();
				}	
				if(compResult == 0 || compResult < 0) {
					String[] temp = checkPolygon(_strTableName);
					if(temp[1].equalsIgnoreCase("true")) {

						int c =Integer.parseInt(temp[0]);
						String pol = ParsePolygon.toString(array[c]);
						System.out.println("PL "+ pol);
						array[c] = pol;
					}
					result.add(array);
				}
			}
		}
		return result;
	}

	public static ArrayList<Object[]> LessThan(DBApp engine, String _strTableName,
			String _strColumnName, 	Object _objValue) throws DBAppException, IOException {
		ArrayList<Object[]> result = new ArrayList<Object[]>();
		Table table = UpdateCheckers.getTable(engine, _strTableName);
		int index = findIndex(_strTableName, _strColumnName);
		int compResult;
		String indexType = indexType(index, _strTableName);
		int pSize = table.getNoOfPages();

		for(int i = 0; i < pSize; i++) {	//loop each page
			Page p = Page.loadPage(_strTableName, i+1);	//get the page

			for(int j = 0; j < p.size(); j++) {
				Object[] array = (Object[]) p.get(j);

				switch(indexType) { 
				case "java.lang.Integer":
					Integer i1 =((Integer) _objValue);
					Integer i2 = (Integer) array[index];
					compResult = i1.compareTo(i2);
					break;
				case "java.lang.String":
					String s1 =((String) _objValue);
					String s2 = (String) array[index]; 
					compResult = s1.compareTo(s2);
					break;
				case "java.lang.Double":
					Double d1 =((Double) _objValue);
					Double d2 = (Double) array[index];
					compResult = d1.compareTo(d2);
					break;
				case "java.awt.Polygon": 
					Polygon p1 = ParsePolygon.returnPolygon((String) _objValue);
					Polygon p2 = (Polygon) array[index];
					compResult = ParsePolygon.compareByArea(p1, p2);
					break;
				case "java.lang.Boolean":  
					Boolean b1 =((Boolean) _objValue);
					Boolean b2 = (Boolean) array[index];
					compResult = b1.compareTo(b2);
					break;
				case "java.util.Date" :  
					Integer d3 =((Integer) _objValue);
					Integer d4 = (Integer) array[index];
					compResult = d3.compareTo(d4);
					break;
				default:
					throw new DBAppException();
				}	
				if(compResult > 0) {
					String[] temp = checkPolygon(_strTableName);
					if(temp[1].equalsIgnoreCase("true")) {

						int c =Integer.parseInt(temp[0]);
						String pol = ParsePolygon.toString(array[c]);
						System.out.println("PL "+ pol);
						array[c] = pol;
					}
					result.add(array);
				}
			}
		}
		return result;
	}

	public static ArrayList<Object[]> LessThanOrEqual(DBApp engine, String _strTableName, 
			String _strColumnName, Object _objValue) throws DBAppException, IOException {
		ArrayList<Object[]> result = new ArrayList<Object[]>();
		Table table = UpdateCheckers.getTable(engine, _strTableName);
		int index = findIndex(_strTableName, _strColumnName);
		int compResult;
		String indexType =indexType(index, _strTableName);
		int pSize = table.getNoOfPages();

		for(int i = 0; i < pSize; i++) {	//loop each page
			Page p = Page.loadPage(_strTableName, i+1);	//get the page

			for(int j = 0; j < p.size(); j++) {
				Object[] array = (Object[]) p.get(j);

				switch(indexType) { 
				case "java.lang.Integer":
					Integer i1 =((Integer) _objValue);
					Integer i2 = (Integer) array[index];
					compResult = i1.compareTo(i2);
					break;
				case "java.lang.String":
					String s1 =((String) _objValue);
					String s2 = (String) array[index]; 
					compResult = s1.compareTo(s2);
					break;
				case "java.lang.Double":
					Double d1 =((Double) _objValue);
					Double d2 = (Double) array[index];
					compResult = d1.compareTo(d2);
					break;
				case "java.awt.Polygon": 
					Polygon p1 = ParsePolygon.returnPolygon((String) _objValue);
					Polygon p2 = (Polygon) array[index];
					compResult = ParsePolygon.compareByArea(p1, p2);
					break;
				case "java.lang.Boolean":  
					Boolean b1 =((Boolean) _objValue);
					Boolean b2 = (Boolean) array[index];
					compResult = b1.compareTo(b2);
					break;
				case "java.util.Date" :  
					Integer d3 =((Integer) _objValue);
					Integer d4 = (Integer) array[index];
					compResult = d3.compareTo(d4);
					break;
				default:
					throw new DBAppException();
				}	
				if(compResult == 0 || compResult > 0) {
					String[] temp = checkPolygon(_strTableName);
					if(temp[1].equalsIgnoreCase("true")) {

						int c =Integer.parseInt(temp[0]);
						String pol = ParsePolygon.toString(array[c]);
						System.out.println("PL "+ pol);
						array[c] = pol;
					}
					result.add(array);
				}
			}
		}
		return result;
	}

	public static ArrayList<Object[]> Equal (DBApp engine, String _strTableName, 
			String _strColumnName, Object _objValue) throws DBAppException, IOException {
		ArrayList<Object[]> result = new ArrayList<Object[]>();
		Table table = UpdateCheckers.getTable(engine, _strTableName);
		int index = findIndex(_strTableName, _strColumnName);
		int compResult;
		String indexType = indexType(index, _strTableName);
		int pSize = table.getNoOfPages();

		for(int i = 0; i < pSize; i++) {	//loop each page
			Page p = Page.loadPage(_strTableName, i+1); //get the page

			for(int j = 0; j < p.size(); j++) {
				Object[] array = (Object[]) p.get(j);

				switch(indexType) { 
				case "java.lang.Integer":
					Integer i1 =((Integer) _objValue);
					Integer i2 = (Integer) array[index];
					compResult = i1.compareTo(i2);
					break;
				case "java.lang.String":
					String s1 =((String) _objValue);
					String s2 = (String) array[index]; 
					compResult = s1.compareTo(s2);
					break;
				case "java.lang.Double":
					Double d1 =((Double) _objValue);
					Double d2 = (Double) array[index];
					compResult = d1.compareTo(d2);
					break;
				case "java.awt.Polygon":
					Polygon p1 = ParsePolygon.returnPolygon((String) _objValue);
					Polygon p2 = (Polygon) array[index];
					Boolean b = ParsePolygon.compareByCoordinates(p1, p2);
					if(b) {
						compResult = 0; 

					}
					else
						compResult = 1;
					break;
				case "java.lang.Boolean":  
					Boolean b1 =((Boolean) _objValue);
					Boolean b2 = (Boolean) array[index];
					compResult = b1.compareTo(b2);
					break;
				case "java.util.Date" :  
					Integer d3 =((Integer) _objValue);
					Integer d4 = (Integer) array[index];
					compResult = d3.compareTo(d4);
					break;
				default:
					throw new DBAppException();
				}	
				if(compResult == 0) {
					String[] temp = checkPolygon(_strTableName);
					if(temp[1].equalsIgnoreCase("true")) {

						int c =Integer.parseInt(temp[0]);
						String pol = ParsePolygon.toString(array[c]);
						System.out.println("PL "+ pol);
						array[c] = pol;
					}
					result.add(array);
				}
			}
		}
//		for(int i = 0; i < result.size(); i++) {
//			System.out.println(result.get(i));
//		}
		return result;
	}

	public static ArrayList<Object[]> NotEqual (DBApp engine, String _strTableName, 
			String _strColumnName, Object _objValue) throws DBAppException, IOException {
		ArrayList<Object[]> result = new ArrayList<Object[]>();
		Table table = UpdateCheckers.getTable(engine, _strTableName);
		int index = findIndex(_strTableName, _strColumnName);
		int compResult;
		String indexType = indexType(index,_strTableName);
		int pSize = table.getNoOfPages();
		for(int i = 0; i < pSize; i++) {	//loop each page
			Page p = Page.loadPage(_strTableName, i+1);	//get the page

			for(int j = 0; j < p.size(); j++) {
				Object[] array = (Object[]) p.get(j);

				switch(indexType) { 
				case "java.lang.Integer":
					Integer i1 =((Integer) _objValue);
					Integer i2 = (Integer) array[index];
					compResult = i1.compareTo(i2);
					break;
				case "java.lang.String":
					String s1 =((String) _objValue);
					String s2 = (String) array[index]; 
					compResult = s1.compareTo(s2);
					break;
				case "java.lang.Double":
					Double d1 =((Double) _objValue);
					Double d2 = (Double) array[index];
					compResult = d1.compareTo(d2);
					break;
				case "java.awt.Polygon": 
					Polygon p1 = ParsePolygon.returnPolygon((String) _objValue);
					Polygon p2 = (Polygon) array[index];
					Boolean b = ParsePolygon.compareByCoordinates(p1, p2);
					if(b)
						compResult = 0; 
					else 
						compResult = 1;
					break;
				case "java.lang.Boolean":  
					Boolean b1 =((Boolean) _objValue);
					Boolean b2 = (Boolean) array[index];
					compResult = b1.compareTo(b2);
					break;
				case "java.util.Date" :  
					Integer d3 =((Integer) _objValue);
					Integer d4 = (Integer) array[index];
					compResult = d3.compareTo(d4);
					break;
				default:
					throw new DBAppException();
				}	
				if(compResult != 0) {
					String[] temp = checkPolygon(_strTableName);
					if(temp[1].equalsIgnoreCase("true")) {

						int c =Integer.parseInt(temp[0]);
						System.out.println(array[c] + "C");
						String pol = ParsePolygon.toString(array[c]);
						System.out.println("POL" + pol);
						array[c] = pol;
					}
					result.add(array);
				}
			}
		}
		return result;
	}

	public static ArrayList<Object[]> indexedNotEqual(DBApp engine, String _strTableName, 
			String _strColumnName, Object _objValue) {
		
		ArrayList<Object[]> result = new ArrayList<Object[]>();
		Table table = UpdateCheckers.getTable(engine, _strTableName);
		
		if(Index.whichTreeToUse(_strTableName, _strColumnName).equalsIgnoreCase("rtree")) {
			RTree rTree = Index.loadRTIndex(_strTableName, _strColumnName);
			Boolean arrayFilled = false;
			RTreeNode node = rTree.root();
			Polygon p = ParsePolygon.returnPolygon(((String)_objValue));

			while(!arrayFilled) {
				if(node.getClass() == rTree.RTreeInnerNode.class) {
					node = ((RTreeInnerNode) node).getFirstChild();
				}
				else if(node.getClass() == rTree.RTreeLeafNode.class) {
					node = ((RTreeLeafNode) node);
					while(node != null) {	
						for(int i = 0; i < node.getNumberOfKeys(); i++) {
							if(!ParsePolygon.compareByCoordinates(p, node.getKey(i))) {
								RRef ref = ((RTreeLeafNode) node).getRecord(i);
//								if(ref instanceof ROverflowPage) {
//									ROverflowPage o = (ROverflowPage) ref;
//									int u = 0;
//									
//									while(o.getNext()!=null) {
//										RRef t = o.getRecords(u);
//										Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
//										result.add(page.get(t.getIndexInPage()));
//										u++;
//										o = o.getNext();
//
//									}
//								}
//								else {
//								Page p = Page.loadPage(table.getTableName(), (ref.getPage()+1));
//								result.add(p.get(ref.getIndexInPage()));
//								}
								if(ref instanceof ROverflowPage) {
									System.out.println("OVERFLOW");
									ROverflowPage o = (ROverflowPage) ref;
									for(int i1 = 0; i1 < o.getRecords().size(); i1++) {
										RRef t = o.getRecords(i1);
										Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
										Object[] temp = page.get(ref.getIndexInPage());
										String[] j = checkPolygon(_strTableName);
									
										if(j[1].equalsIgnoreCase("true")) {

											int c =Integer.parseInt(j[0]);
											String pol = ParsePolygon.toString(temp[c]);
											System.out.println("PL "+ pol);
											temp[c] = pol;
										}
										
										result.add(temp);
									}
								}
								else {
								Page page = Page.loadPage(table.getTableName(), (ref.getPage()+1));
								Object[] temp = page.get(ref.getIndexInPage());
								String[] t = checkPolygon(_strTableName);
							
								if(t[1].equalsIgnoreCase("true")) {

									int c =Integer.parseInt(t[0]);
									String pol = ParsePolygon.toString(temp[c]);
									System.out.println("PL "+ pol);
									temp[c] = pol;
								}
								
								result.add(temp);
								}
							}
						}
						node = ((RTreeLeafNode) node).getNext();
					}
					arrayFilled = true;
				}
			}
		}
		else if(Index.whichTreeToUse(_strTableName, _strColumnName).equalsIgnoreCase("bplustree")) {
		System.out.println("BTree");
		BPTree bTree = Index.loadBTIndex(_strTableName, _strColumnName);
		Boolean arrayFilled = false;
		BPTreeNode node = bTree.root();
		
		while(!arrayFilled) {
			if(node.getClass() == bPlusTree.BPTreeInnerNode.class) {
				node = ((BPTreeInnerNode) node).getFirstChild();
			}
			else if(node.getClass() == bPlusTree.BPTreeLeafNode.class) {
				node = ((BPTreeLeafNode) node);
				while(node != null) {
					for(int i = 0; i < node.getNumberOfKeys(); i++) {
						if(((Comparable) _objValue).compareTo(node.getKey(i)) != 0) {
							Ref ref = ((BPTreeLeafNode) node).getRecord(i);
//							if(ref instanceof OverflowPage) {
//								OverflowPage o = (OverflowPage) ref;
//								int u = 0;
//								
//								while(o.getNext()!=null) {
//									Ref t = o.getRecords(u);
//									Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
//									result.add(page.get(t.getIndexInPage()));
//									u++;
//									o = o.getNext();
//
//								}
//							}
//							else {
//							Page p = Page.loadPage(table.getTableName(), (ref.getPage()+1));
//							result.add(p.get(ref.getIndexInPage()));
//							}
							if(ref instanceof OverflowPage) {
								System.out.println("OVERFLOW");
								OverflowPage o = (OverflowPage) ref;
								for(int i1 = 0; i1 < o.getRecords().size(); i1++) {
									Ref t = o.getRecords(i1);
									Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
									Object[] temp = page.get(ref.getIndexInPage());
									String[] j = checkPolygon(_strTableName);
								
									if(j[1].equalsIgnoreCase("true")) {

										int c =Integer.parseInt(j[0]);
										String pol = ParsePolygon.toString(temp[c]);
										System.out.println("PL "+ pol);
										temp[c] = pol;
									}
									
									result.add(temp);
								}
							}
							else {
							Page page = Page.loadPage(table.getTableName(), (ref.getPage()+1));
							Object[] temp = page.get(ref.getIndexInPage());
							String[] j = checkPolygon(_strTableName);
						
							if(j[1].equalsIgnoreCase("true")) {

								int c =Integer.parseInt(j[0]);
								String pol = ParsePolygon.toString(temp[c]);
								System.out.println("PL "+ pol);
								temp[c] = pol;
							}
							
							result.add(temp);
							}
						}
					}
					node = ((BPTreeLeafNode) node).getNext();
				}
				arrayFilled = true;
			}
		}
	}
		return result;
	}
	
	public static ArrayList<Object[]> indexedEqual(DBApp engine, String _strTableName, 
			String _strColumnName, Object _objValue) throws IOException, DBAppException {
		
		ArrayList<Object[]> result = new ArrayList<Object[]>();	
		Table table = UpdateCheckers.getTable(engine, _strTableName);
		if(Index.whichTreeToUse(_strTableName, _strColumnName).equalsIgnoreCase("rtree")) {
			RTree rTree = Index.loadRTIndex(_strTableName, _strColumnName);
			Polygon p = ParsePolygon.returnPolygon(((String)_objValue));
			RTreeNode node = rTree.root();
			Boolean stop = false; 
			
			while(!stop) {
				if(node.getClass() == rTree.RTreeInnerNode.class) {
					node = ((RTreeInnerNode) node).getFirstChild();
				}
				else if(node.getClass() == rTree.RTreeLeafNode.class) {
					node = ((RTreeLeafNode) node);
					while(node != null) {
						for(int i = 0; i < node.getNumberOfKeys(); i++) {
							if(ParsePolygon.compareByCoordinates(p, node.getKey(i))) {
								RRef ref = ((RTreeLeafNode) node).getRecord(i);
//								if(ref instanceof ROverflowPage) {
//									ROverflowPage o = (ROverflowPage) ref;
//									int u = 0;
//									
//									while(o.getNext()!=null) {
//										RRef t = o.getRecords(u);
//										Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
//										result.add(page.get(t.getIndexInPage()));
//										u++;
//										o = o.getNext();
//
//									}
//								}
//								else {
//								Page p = Page.loadPage(table.getTableName(), (ref.getPage()+1));
//								result.add(p.get(ref.getIndexInPage()));
//								}
								if(ref instanceof ROverflowPage) {
									ROverflowPage o = (ROverflowPage) ref;
									for(int i1 = 0; i1 < o.getRecords().size(); i1++) {
										RRef t = o.getRecords(i1);
										Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
										Object[] temp = page.get(ref.getIndexInPage());
										String[] j = checkPolygon(_strTableName);
									
										if(j[1].equalsIgnoreCase("true")) {

											int c =Integer.parseInt(j[0]);
											String pol = ParsePolygon.toString(temp[c]);
											System.out.println("PL "+ pol);
											temp[c] = pol;
										}
										
										result.add(temp);
									}
								}
								else {
								
									System.out.println("hi");
								Page page = Page.loadPage(table.getTableName(), (ref.getPage()+1));
								Object[] temp = page.get(ref.getIndexInPage());
								String[] t = checkPolygon(_strTableName);
							
								if(t[1].equalsIgnoreCase("true")) {

									int c =Integer.parseInt(t[0]);
									String pol = ParsePolygon.toString(temp[c]);
									System.out.println("PL "+ pol);
									temp[c] = pol;
								}
								
								result.add(temp);
								}
							}

						}
						node = ((RTreeLeafNode) node).getNext();
					}
					stop = true;
				}
			
			RRef ref = rTree.search(p);
//			if(ref == null) {
//				return result;
//			}
//			if(ref != null) {
//				if(ref instanceof ROverflowPage) {
//					ROverflowPage o = (ROverflowPage) ref;
//					int u = 0;
//					
//					while(o.getNext()!=null) {
//						RRef t = o.getRecords(u);
//						Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
//						result.add(page.get(t.getIndexInPage()));
//						u++;
//						o = o.getNext();
//
//					}
//				}
//				else {
//				Page page = Page.loadPage(table.getTableName(), (ref.getPage()+1));
//				result.add(page.get(ref.getIndexInPage()));
//				}
			}
		}
		else if(Index.whichTreeToUse(_strTableName, _strColumnName).equalsIgnoreCase("bplustree")) {
		BPTree bTree = Index.loadBTIndex(_strTableName, _strColumnName);

		Ref ref = bTree.search((Comparable)_objValue);

		if(ref == null) {
			System.out.println("NO REF");
			return result;
		}

		if(ref != null) {

			if(ref instanceof OverflowPage) {
				System.out.println("OVERFLOW");
				OverflowPage o = (OverflowPage) ref;
				for(int i = 0; i < o.getRecords().size(); i++) {
					Ref t = o.getRecords(i);
					Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
					Object[] temp = page.get(ref.getIndexInPage());
					String[] j = checkPolygon(_strTableName);
				
					if(j[1].equalsIgnoreCase("true")) {

						int c =Integer.parseInt(j[0]);
						String pol = ParsePolygon.toString(temp[c]);
						System.out.println("PL "+ pol);
						temp[c] = pol;
					}
					
					result.add(temp);				}
			}
			else {
			Page page = Page.loadPage(table.getTableName(), (ref.getPage()+1));
			Object[] temp = page.get(ref.getIndexInPage());
			String[] j = checkPolygon(_strTableName);
		
			if(j[1].equalsIgnoreCase("true")) {

				int c =Integer.parseInt(j[0]);
				String pol = ParsePolygon.toString(temp[c]);
				System.out.println("PL "+ pol);
				temp[c] = pol;
			}
			
			result.add(temp);			}
		}
		}
		return result;
			}
	
	public static ArrayList<Object[]> indexedLessThanOrEqual(DBApp engine, String _strTableName, 
			String _strColumnName, Object _objValue) {
		
		ArrayList<Object[]> result = new ArrayList<Object[]>();
		Table table = UpdateCheckers.getTable(engine, _strTableName);
		
		if(Index.whichTreeToUse(_strTableName, _strColumnName).equalsIgnoreCase("rtree")) {
			RTree rTree = Index.loadRTIndex(_strTableName, _strColumnName);
			Boolean arrayFilled = false;
			RTreeNode node = rTree.root();
			
			Polygon p = ParsePolygon.returnPolygon(((String)_objValue));

			while(!arrayFilled) {
				if(node.getClass() == rTree.RTreeInnerNode.class) {
					node = ((RTreeInnerNode) node).getFirstChild();
				}
				else if(node.getClass() == rTree.RTreeLeafNode.class) {
					node = ((RTreeLeafNode) node);
					while(node != null) {
						for(int i = 0; i < node.getNumberOfKeys(); i++) {
							if(ParsePolygon.compareByArea(p, node.getKey(i)) >= 0) {
								RRef ref = ((RTreeLeafNode) node).getRecord(i);
//								if(ref instanceof ROverflowPage) {
//									ROverflowPage o = (ROverflowPage) ref;
//									int u = 0;
//									
//									while(o.getNext()!=null) {
//										RRef t = o.getRecords(u);
//										Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
//										result.add(page.get(t.getIndexInPage()));
//										u++;
//										o = o.getNext();
//
//									}
//								}
//								else {
//								Page p = Page.loadPage(table.getTableName(), (ref.getPage()+1));
//								result.add(p.get(ref.getIndexInPage()));
//								}
								if(ref instanceof ROverflowPage) {
									System.out.println("OVERFLOW");
									ROverflowPage o = (ROverflowPage) ref;
									for(int i1 = 0; i1 < o.getRecords().size(); i1++) {
										RRef t = o.getRecords(i1);
										Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
										Object[] temp = page.get(ref.getIndexInPage());
										String[] j = checkPolygon(_strTableName);
									
										if(j[1].equalsIgnoreCase("true")) {

											int c =Integer.parseInt(j[0]);
											String pol = ParsePolygon.toString(temp[c]);
											System.out.println("PL "+ pol);
											temp[c] = pol;
										}
										
										result.add(temp);									}
								}
								else {
								Page page = Page.loadPage(table.getTableName(), (ref.getPage()+1));
								Object[] temp = page.get(ref.getIndexInPage());
								String[] j = checkPolygon(_strTableName);
							
								if(j[1].equalsIgnoreCase("true")) {

									int c =Integer.parseInt(j[0]);
									String pol = ParsePolygon.toString(temp[c]);
									System.out.println("PL "+ pol);
									temp[c] = pol;
								}
								
								result.add(temp);								}
							}
							else {
								arrayFilled = true;
							}
						}
						node = ((RTreeLeafNode) node).getNext();
					}
					arrayFilled = true;
				}
			}
		}
		else if(Index.whichTreeToUse(_strTableName, _strColumnName).equalsIgnoreCase("bplustree")) {

		BPTree bTree = Index.loadBTIndex(_strTableName, _strColumnName);
		Boolean arrayFilled = false;
		BPTreeNode node = bTree.root();
		System.out.println("root " + node);

		while(!arrayFilled) {
			if(node.getClass() == bPlusTree.BPTreeInnerNode.class) {
				node = ((BPTreeInnerNode) node).getFirstChild();
			}
			else if(node.getClass() == bPlusTree.BPTreeLeafNode.class) {
				node = ((BPTreeLeafNode) node);
				System.out.println("1st child " + node.toString());

				while(node != null) {
					for(int i = 0; i < node.getNumberOfKeys(); i++) {
						if(((Comparable) _objValue).compareTo(node.getKey(i)) >= 0) {
							System.out.println(node.getKey(i));
							System.out.println("yes");
							Ref ref = ((BPTreeLeafNode) node).getRecord(i);
//							if(ref instanceof OverflowPage) {
//								OverflowPage o = (OverflowPage) ref;
//								int u = 0;
//								
//								while(o.getNext()!=null) {
//									Ref t = o.getRecords(u);
//									Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
//									result.add(page.get(t.getIndexInPage()));
//									u++;
//									o = o.getNext();
//
//								}
//							}
//							else {
//							Page p = Page.loadPage(table.getTableName(), (ref.getPage()+1));
//							result.add(p.get(ref.getIndexInPage()));
//							}
							if(ref instanceof OverflowPage) {
								System.out.println("OVERFLOW");
								OverflowPage o = (OverflowPage) ref;
								for(int i1 = 0; i1 < o.getRecords().size(); i1++) {
									Ref t = o.getRecords(i1);
									Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
									Object[] temp = page.get(ref.getIndexInPage());
									String[] j = checkPolygon(_strTableName);
								
									if(j[1].equalsIgnoreCase("true")) {

										int c =Integer.parseInt(j[0]);
										String pol = ParsePolygon.toString(temp[c]);
										System.out.println("PL "+ pol);
										temp[c] = pol;
									}
									
									result.add(temp);								}
							}
							else {
							Page page = Page.loadPage(table.getTableName(), (ref.getPage()+1));
							Object[] temp = page.get(ref.getIndexInPage());
							String[] j = checkPolygon(_strTableName);
						
							if(j[1].equalsIgnoreCase("true")) {

								int c =Integer.parseInt(j[0]);
								String pol = ParsePolygon.toString(temp[c]);
								System.out.println("PL "+ pol);
								temp[c] = pol;
							}
							
							result.add(temp);							}
						}
						else {
							arrayFilled = true;
						}
					}
					node = ((BPTreeLeafNode) node).getNext();
				}
				arrayFilled = true;
			}
		}
		}
		return result;
	}
	
	public static ArrayList<Object[]> indexedLessThan(DBApp engine, String _strTableName, 
			String _strColumnName, Object _objValue) {
		ArrayList<Object[]> result = new ArrayList<Object[]>();
		Table table = UpdateCheckers.getTable(engine, _strTableName);
		if(Index.whichTreeToUse(_strTableName, _strColumnName).equalsIgnoreCase("rtree")) {
			RTree rTree = Index.loadRTIndex(_strTableName, _strColumnName);
			Boolean arrayFilled = false;
			RTreeNode node = rTree.root();
			Polygon p = ParsePolygon.returnPolygon(((String)_objValue));

			while(!arrayFilled) {
				if(node.getClass() == rTree.RTreeInnerNode.class) {
					node = ((RTreeInnerNode) node).getFirstChild();
				}
				else if(node.getClass() == rTree.RTreeLeafNode.class) {
					node = ((RTreeLeafNode) node);
					while(node != null) {
						for(int i = 0; i < node.getNumberOfKeys(); i++) {
							if(ParsePolygon.compareByArea(p, node.getKey(i)) > 0) {
								RRef ref = ((RTreeLeafNode) node).getRecord(i);
//								if(ref instanceof ROverflowPage) {
//									ROverflowPage o = (ROverflowPage) ref;
//									int u = 0;
//									
//									while(o.getNext()!=null) {
//										RRef t = o.getRecords(u);
//										Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
//										result.add(page.get(t.getIndexInPage()));
//										u++;
//										o = o.getNext();
//
//									}
//								}
//								else {
//								Page p = Page.loadPage(table.getTableName(), (ref.getPage()+1));
//								result.add(p.get(ref.getIndexInPage()));
//								}
								if(ref instanceof ROverflowPage) {
									System.out.println("OVERFLOW");
									ROverflowPage o = (ROverflowPage) ref;
									for(int i1 = 0; i1 < o.getRecords().size(); i1++) {
										RRef t = o.getRecords(i1);
										Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
										Object[] temp = page.get(ref.getIndexInPage());
										String[] j = checkPolygon(_strTableName);
									
										if(j[1].equalsIgnoreCase("true")) {

											int c =Integer.parseInt(j[0]);
											String pol = ParsePolygon.toString(temp[c]);
											System.out.println("PL "+ pol);
											temp[c] = pol;
										}
										
										result.add(temp);									}
								}
								else {
								Page page = Page.loadPage(table.getTableName(), (ref.getPage()+1));
								Object[] temp = page.get(ref.getIndexInPage());
								String[] j = checkPolygon(_strTableName);
							
								if(j[1].equalsIgnoreCase("true")) {

									int c =Integer.parseInt(j[0]);
									String pol = ParsePolygon.toString(temp[c]);
									System.out.println("PL "+ pol);
									temp[c] = pol;
								}
								
								result.add(temp);								}
							}
							else {
								arrayFilled = true;
							}
						}
						node = ((RTreeLeafNode) node).getNext();
					}
					arrayFilled = true;
				}
			}
		}
		else if(Index.whichTreeToUse(_strTableName, _strColumnName).equalsIgnoreCase("bplustree")) {

		BPTree bTree = Index.loadBTIndex(_strTableName, _strColumnName);
		Boolean arrayFilled = false;
		BPTreeNode node = bTree.root();
		
		while(!arrayFilled) {
			if(node.getClass() == bPlusTree.BPTreeInnerNode.class) {
				node = ((BPTreeInnerNode) node).getFirstChild();
			}
			else if(node.getClass() == bPlusTree.BPTreeLeafNode.class) {
				node = ((BPTreeLeafNode) node);
				while(node != null) {
					for(int i = 0; i < node.getNumberOfKeys(); i++) {
						if(((Comparable) _objValue).compareTo(node.getKey(i)) > 0) {
							Ref ref = ((BPTreeLeafNode) node).getRecord(i);
//							if(ref instanceof OverflowPage) {
//								OverflowPage o = (OverflowPage) ref;
//								int u = 0;
//								
//								while(o.getNext()!=null) {
//									Ref t = o.getRecords(u);
//									Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
//									result.add(page.get(t.getIndexInPage()));
//									u++;
//									o = o.getNext();
//
//								}
//							}
//							else {
//							Page p = Page.loadPage(table.getTableName(), (ref.getPage()+1));
//							result.add(p.get(ref.getIndexInPage()));
//							}
							if(ref instanceof OverflowPage) {
								System.out.println("OVERFLOW");
								OverflowPage o = (OverflowPage) ref;
								for(int i1 = 0; i1 < o.getRecords().size(); i1++) {
									Ref t = o.getRecords(i1);
									Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
									Object[] temp = page.get(ref.getIndexInPage());
									String[] j = checkPolygon(_strTableName);
								
									if(j[1].equalsIgnoreCase("true")) {

										int c =Integer.parseInt(j[0]);
										String pol = ParsePolygon.toString(temp[c]);
										System.out.println("PL "+ pol);
										temp[c] = pol;
									}
									
									result.add(temp);								}
							}
							else {
							Page page = Page.loadPage(table.getTableName(), (ref.getPage()+1));
							Object[] temp = page.get(ref.getIndexInPage());
							String[] j = checkPolygon(_strTableName);
						
							if(j[1].equalsIgnoreCase("true")) {

								int c =Integer.parseInt(j[0]);
								String pol = ParsePolygon.toString(temp[c]);
								System.out.println("PL "+ pol);
								temp[c] = pol;
							}
							
							result.add(temp);							}
						}
						else {
							arrayFilled = true;
						}
					}
					node = ((BPTreeLeafNode) node).getNext();
				}
				arrayFilled = true;
			}
		}
		}
		return result;
	}
	
	public static ArrayList<Object[]> indexedGreaterThanOrEqual (DBApp engine, String _strTableName, 
			String _strColumnName, Object _objValue) {
		ArrayList<Object[]> result = new ArrayList<Object[]>();
		Table table = UpdateCheckers.getTable(engine, _strTableName);
		
		if(Index.whichTreeToUse(_strTableName, _strColumnName).equalsIgnoreCase("rtree")) {
			RTree rTree = Index.loadRTIndex(_strTableName, _strColumnName);
			Boolean arrayFilled = false;
			RTreeNode node = rTree.root();
			Boolean stop = false;
			Polygon p = ParsePolygon.returnPolygon(((String)_objValue));

			
			while(!arrayFilled) {		
				Boolean childFound = false;
				if(node.getClass() == rTree.RTreeInnerNode.class) {
					for(int i = 0; i < node.getNumberOfKeys() && !childFound; i++) {
						Polygon temp = node.getKey(i);

						if(ParsePolygon.compareByArea(temp, p) == 0) {
							node = ((RTreeInnerNode) node).getChild(i+1);
							childFound = true;
							break;
						}

						if(ParsePolygon.compareByArea(temp, p) < 0) {

						}

						if(ParsePolygon.compareByArea(temp, p) > 0) {
							node = ((RTreeInnerNode) node).getChild(i);
							childFound = true;
							break;
						}

						if(!childFound) {
							node = ((RTreeInnerNode) node).getChild(i);
							childFound = true;
						}
					}
				}
				else if(node.getClass() == rTree.RTreeLeafNode.class) {
					node = ((RTreeLeafNode) node);
					
					while(node != null) {
						
						for(int i = 0; i < node.getNumberOfKeys(); i++) {
							if(ParsePolygon.compareByArea(p, node.getKey(i)) < 0 || 
									ParsePolygon.compareByArea(p, node.getKey(i)) == 0) {
								RRef ref = ((RTreeLeafNode) node).getRecord(i);
//								if(ref instanceof ROverflowPage) {
//									ROverflowPage o = (ROverflowPage) ref;
//									int u = 0;
//									
//									while(o.getNext()!=null) {
//										RRef t = o.getRecords(u);
//										Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
//										result.add(page.get(t.getIndexInPage()));
//										u++;
//										o = o.getNext();
//
//									}
//								}
//								else {
//									Page p = Page.loadPage(table.getTableName(), (ref.getPage()+1));
//									result.add(p.get(ref.getIndexInPage()));
//								}
								if(ref instanceof ROverflowPage) {
									System.out.println("OVERFLOW");
									ROverflowPage o = (ROverflowPage) ref;
									for(int i1 = 0; i1 < o.getRecords().size(); i1++) {
										RRef t = o.getRecords(i1);
										Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
										Object[] temp = page.get(ref.getIndexInPage());
										String[] j = checkPolygon(_strTableName);
									
										if(j[1].equalsIgnoreCase("true")) {

											int c =Integer.parseInt(j[0]);
											String pol = ParsePolygon.toString(temp[c]);
											System.out.println("PL "+ pol);
											temp[c] = pol;
										}
										
										result.add(temp);									}
								}
								else {
								Page page = Page.loadPage(table.getTableName(), (ref.getPage()+1));
								Object[] temp = page.get(ref.getIndexInPage());
								String[] j = checkPolygon(_strTableName);
							
								if(j[1].equalsIgnoreCase("true")) {

									int c =Integer.parseInt(j[0]);
									String pol = ParsePolygon.toString(temp[c]);
									System.out.println("PL "+ pol);
									temp[c] = pol;
								}
								
								result.add(temp);								}
							}

						}
						node = ((RTreeLeafNode) node).getNext();

					}
					arrayFilled = true;
				}
			}
		}
		else if(Index.whichTreeToUse(_strTableName, _strColumnName).equalsIgnoreCase("bplustree")) {

		BPTree bTree = Index.loadBTIndex(_strTableName, _strColumnName);
		Boolean arrayFilled = false;
		BPTreeNode node = bTree.root();
		Boolean stop = false;
		
		while(!arrayFilled) {		
			Boolean childFound = false;
			if(node.getClass() == bPlusTree.BPTreeInnerNode.class) {
				for(int i = 0; i < node.getNumberOfKeys() && !childFound; i++) {
					Comparable temp = node.getKey(i);

					if(temp.compareTo(_objValue) == 0) {
						node = ((BPTreeInnerNode) node).getChild(i+1);
						childFound = true;
						break;
					}

					if(temp.compareTo(_objValue) < 0) {

					}

					if(temp.compareTo(_objValue) > 0) {
						node = ((BPTreeInnerNode) node).getChild(i);
						childFound = true;
						break;
					}

					if(!childFound) {
						node = ((BPTreeInnerNode) node).getChild(i);
						childFound = true;
					}
				}
			}
			else if(node.getClass() == bPlusTree.BPTreeLeafNode.class) {
				node = ((BPTreeLeafNode) node);
				
				while(node != null) {
					
					for(int i = 0; i < node.getNumberOfKeys(); i++) {
						if(((Comparable) _objValue).compareTo(node.getKey(i)) < 0 ||
								((Comparable) _objValue).compareTo(node.getKey(i)) == 0) {
							Ref ref = ((BPTreeLeafNode) node).getRecord(i);
//							if(ref instanceof OverflowPage) {
//								OverflowPage o = (OverflowPage) ref;
//								int u = 0;
//								
//								while(o.getNext()!=null) {
//									Ref t = o.getRecords(u);
//									Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
//									result.add(page.get(t.getIndexInPage()));
//									u++;
//									o = o.getNext();
//
//								}
//							}
//							else {
//								Page p = Page.loadPage(table.getTableName(), (ref.getPage()+1));
//								result.add(p.get(ref.getIndexInPage()));
//							}
							if(ref instanceof OverflowPage) {
								System.out.println("OVERFLOW");
								OverflowPage o = (OverflowPage) ref;
								for(int i1 = 0; i1 < o.getRecords().size(); i1++) {
									Ref t = o.getRecords(i1);
									Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
									Object[] temp = page.get(ref.getIndexInPage());
									String[] j = checkPolygon(_strTableName);
								
									if(j[1].equalsIgnoreCase("true")) {

										int c =Integer.parseInt(j[0]);
										String pol = ParsePolygon.toString(temp[c]);
										System.out.println("PL "+ pol);
										temp[c] = pol;
									}
									
									result.add(temp);								}
							}
							else {
							Page page = Page.loadPage(table.getTableName(), (ref.getPage()+1));
							Object[] temp = page.get(ref.getIndexInPage());
							String[] j = checkPolygon(_strTableName);
						
							if(j[1].equalsIgnoreCase("true")) {

								int c =Integer.parseInt(j[0]);
								String pol = ParsePolygon.toString(temp[c]);
								System.out.println("PL "+ pol);
								temp[c] = pol;
							}
							
							result.add(temp);							}
						}

					}
					node = ((BPTreeLeafNode) node).getNext();

				}
				arrayFilled = true;
			}
		}
		}
		return result;
	
	}
	
	public static ArrayList<Object[]> indexedGreaterThan(DBApp engine, String _strTableName, 
			String _strColumnName, Object _objValue) {
		ArrayList<Object[]> result = new ArrayList<Object[]>();
		Table table = UpdateCheckers.getTable(engine, _strTableName);

		if(Index.whichTreeToUse(_strTableName, _strColumnName).equalsIgnoreCase("rtree")) {
			RTree rTree = Index.loadRTIndex(_strTableName, _strColumnName);
			Boolean arrayFilled = false;
			RTreeNode node = rTree.root();
			Boolean stop = false;
			Polygon p = ParsePolygon.returnPolygon(((String)_objValue));

			
			while(!arrayFilled) {		
				Boolean childFound = false;
				if(node.getClass() == rTree.RTreeInnerNode.class) {
					for(int i = 0; i < node.getNumberOfKeys() && !childFound; i++) {
						Polygon temp = node.getKey(i);

						if(ParsePolygon.compareByArea(temp, p)  == 0) {
							node = ((RTreeInnerNode) node).getChild(i+1);
							childFound = true;
							break;
						}

						if(ParsePolygon.compareByArea(temp, p) < 0) {

						}

						if(ParsePolygon.compareByArea(temp, p) > 0) {
							node = ((RTreeInnerNode) node).getChild(i);
							childFound = true;
							break;
						}

						if(!childFound) {
							node = ((RTreeInnerNode) node).getChild(i);
							childFound = true;
						}
					}
				}
				else if(node.getClass() == rTree.RTreeLeafNode.class) {
					node = ((RTreeLeafNode) node);
					
					while(node != null) {
						
						for(int i = 0; i < node.getNumberOfKeys(); i++) {
							if(ParsePolygon.compareByArea(p, node.getKey(i)) < 0) {
								RRef ref = ((RTreeLeafNode) node).getRecord(i);
//								if(ref instanceof ROverflowPage) {
//									ROverflowPage o = (ROverflowPage) ref;
//									int u = 0;
//									
//									while(o.getNext()!=null) {
//										RRef t = o.getRecords(u);
//										Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
//										result.add(page.get(t.getIndexInPage()));
//										u++;
//										o = o.getNext();
//
//									}
//								}
//								else {
//									Page p = Page.loadPage(table.getTableName(), (ref.getPage()+1));
//									result.add(p.get(ref.getIndexInPage()));
//								}
								if(ref instanceof ROverflowPage) {
									System.out.println("OVERFLOW");
									ROverflowPage o = (ROverflowPage) ref;
									for(int i1 = 0; i1 < o.getRecords().size(); i1++) {
										RRef t = o.getRecords(i1);
										Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
										Object[] temp = page.get(ref.getIndexInPage());
										String[] j = checkPolygon(_strTableName);
									
										if(j[1].equalsIgnoreCase("true")) {

											int c =Integer.parseInt(j[0]);
											String pol = ParsePolygon.toString(temp[c]);
											System.out.println("PL "+ pol);
											temp[c] = pol;
										}
										
										result.add(temp);									}
								}
								else {
								Page page = Page.loadPage(table.getTableName(), (ref.getPage()+1));
								Object[] temp = page.get(ref.getIndexInPage());
								String[] j = checkPolygon(_strTableName);
							
								if(j[1].equalsIgnoreCase("true")) {

									int c =Integer.parseInt(j[0]);
									String pol = ParsePolygon.toString(temp[c]);
									System.out.println("PL "+ pol);
									temp[c] = pol;
								}
								
								result.add(temp);								}
							}

						}
						node = ((RTreeLeafNode) node).getNext();
					}
					arrayFilled = true;
				}
			}
		}
		else if(Index.whichTreeToUse(_strTableName, _strColumnName).equalsIgnoreCase("bplustree")) {
		
		BPTree bTree = Index.loadBTIndex(_strTableName, _strColumnName);
		Boolean arrayFilled = false;
		BPTreeNode node = bTree.root();
		Boolean stop = false;
		
		while(!arrayFilled) {		
			Boolean childFound = false;
			if(node.getClass() == bPlusTree.BPTreeInnerNode.class) {
				for(int i = 0; i < node.getNumberOfKeys() && !childFound; i++) {
					Comparable temp = node.getKey(i);

					if(temp.compareTo(_objValue) == 0) {
						node = ((BPTreeInnerNode) node).getChild(i+1);
						childFound = true;
						break;
					}

					if(temp.compareTo(_objValue) < 0) {

					}

					if(temp.compareTo(_objValue) > 0) {
						node = ((BPTreeInnerNode) node).getChild(i);
						childFound = true;
						break;
					}

					if(!childFound) {
						node = ((BPTreeInnerNode) node).getChild(i);
						childFound = true;
					}
				}
			}
			else if(node.getClass() == bPlusTree.BPTreeLeafNode.class) {
				node = ((BPTreeLeafNode) node);
				
				while(node != null) {
					
					for(int i = 0; i < node.getNumberOfKeys(); i++) {
						if(((Comparable) _objValue).compareTo(node.getKey(i)) < 0) {
							Ref ref = ((BPTreeLeafNode) node).getRecord(i);
//							if(ref instanceof OverflowPage) {
//								OverflowPage o = (OverflowPage) ref;
//								int u = 0;
//								
//								while(o.getNext()!=null) {
//									Ref t = o.getRecords(u);
//									Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
//									result.add(page.get(t.getIndexInPage()));
//									u++;
//									o = o.getNext();
//
//								}
//							}
//							else {
//								Page p = Page.loadPage(table.getTableName(), (ref.getPage()+1));
//								result.add(p.get(ref.getIndexInPage()));
//							}
							if(ref instanceof OverflowPage) {
								System.out.println("OVERFLOW");
								OverflowPage o = (OverflowPage) ref;
								for(int i1 = 0; i1 < o.getRecords().size(); i1++) {
									Ref t = o.getRecords(i1);
									Page page = Page.loadPage(table.getTableName(), (t.getPage()+1));
									Object[] temp = page.get(ref.getIndexInPage());
									String[] j = checkPolygon(_strTableName);
								
									if(j[1].equalsIgnoreCase("true")) {

										int c =Integer.parseInt(j[0]);
										String pol = ParsePolygon.toString(temp[c]);
										System.out.println("PL "+ pol);
										temp[c] = pol;
									}
									
									result.add(temp);								}
							}
							else {
							Page page = Page.loadPage(table.getTableName(), (ref.getPage()+1));
							Object[] temp = page.get(ref.getIndexInPage());
							String[] j = checkPolygon(_strTableName);
						
							if(j[1].equalsIgnoreCase("true")) {

								int c =Integer.parseInt(j[0]);
								String pol = ParsePolygon.toString(temp[c]);
								System.out.println("PL "+ pol);
								temp[c] = pol;
							}
							
							result.add(temp);							}
						}

					}
					node = ((BPTreeLeafNode) node).getNext();
				}
				arrayFilled = true;
			}
		}
		}
		return result;
		
	}

	public static ArrayList<Object[]> XOR(ArrayList<Object[]> y, ArrayList<Object[]> z) {		
		ArrayList<Object[]> result = new ArrayList<Object[]>();
		
		for(int j = 0; j < z.size(); j++) {
			boolean check = true;
			for(int i = 0; i < y.size(); i++) {
				if(Arrays.equals(y.get(i), z.get(j))) {
					check = false;
				}
			}
			if(check) {
				result.add(z.get(j));
			}
		}
		
		for(int j = 0; j < y.size(); j++) {
			boolean check = true;
			for(int i = 0; i < z.size(); i++) {
				if(Arrays.equals(z.get(i), y.get(j))) {
					check = false;
				}
			}
			if(check) {
				result.add(y.get(j));
			}
		}
		
		return result;
	}		

	public static ArrayList<Object[]> AND(ArrayList<Object[]> y, ArrayList<Object[]> z) {
		ArrayList<Object[]> result = new ArrayList<Object[]>();




		for(int j = 0; j < z.size(); j++) {
			boolean check = false;
			for(int i = 0; i < y.size(); i++) {
				if(Arrays.equals(y.get(i), z.get(j))) {
					check = true;
				}
			}
			if(check) {
				result.add(z.get(j));
			}
		}

		return result;
	}	

	public static ArrayList<Object[]> OR(ArrayList<Object[]> y, ArrayList<Object[]> z) {
		ArrayList<Object[]> result = new ArrayList<Object[]>();

		for(int j = 0; j < y.size(); j++) {
			result.add(y.get(j));
		}	
		
		for(int j = 0; j < z.size(); j++) {
			boolean check = true;
			for(int i = 0; i < y.size(); i++) {
				if(Arrays.equals(y.get(i), z.get(j))) {
					check = false;
				}
			}
			if(check)
				result.add(z.get(j));
		}


		return result;
	}
	
	
	public static int checkOperators(String[] strarrOperators) {
		if(strarrOperators.length == 0) {
			return 1;
		}

			for(int i = 0; i < strarrOperators.length; i++) {
				if(strarrOperators[i].equalsIgnoreCase("or") || 
						strarrOperators[i].equalsIgnoreCase("xor") ||
							strarrOperators[i].equalsIgnoreCase("and")) {
					//continue
				}
				else 
					return 2;	//false
			}
			return 3; 		//true
		}

	public static boolean checkColumnNames(DBApp engine, SQLTerm[] arrSQLTerms) throws DBAppException {
		ArrayList<String> inputColumnNames = new ArrayList<String>();
		Table table = UpdateCheckers.getTable(engine, arrSQLTerms[0]._strTableName);
		ArrayList<String> tableColumnNames = table.getColNames();

		if(table == null) {
			throw new DBAppException("The table you are trying to update does not exist.");
		}	
		
		for(int i = 0; i < arrSQLTerms.length; i++) {
			inputColumnNames.add(arrSQLTerms[i]._strColumnName);
		}
		for(String inputName : inputColumnNames) {
			if(!tableColumnNames.contains(inputName))
				return false;
		}
		return true;
	}
	@SuppressWarnings("resource")
	public static String[] checkPolygon(String strTableName) {
		try {
			int indx = 0;
			String line = null;			
			BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
			while ((line = br.readLine()) != null) {		
				String[] content = line.split(",");
				if(strTableName.equals(content[0])) {		
					if(content[2].equalsIgnoreCase("java.awt.Polygon")) {	//check this condition
						String[] x = new String[2];					
						x[0] = Integer.toString(indx);
						x[1] = "true";
						return x;
				}
					indx++;
			}
			}
			br.close();
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	return null;
	}
	
 }