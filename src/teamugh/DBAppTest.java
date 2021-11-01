
package teamugh;

import java.awt.Polygon;
import java.io.IOException;
import java.util.Hashtable;

import bPlusTree.BPTree;
import bPlusTree.Ref;
import rTree.RTree;
import rTree.RRef;

@SuppressWarnings("unused")
public class DBAppTest {

	@SuppressWarnings("rawtypes")
	public static void main (String [] args) throws DBAppException, IOException, ClassNotFoundException {
		DBApp a = new DBApp ();
		a.init();

		Hashtable<String,String> htblColNameType = new Hashtable<String,String>();
		htblColNameType.put("name","java.lang.String");
		htblColNameType.put("salary","java.lang.Double");
		htblColNameType.put("id","java.lang.Integer");
		htblColNameType.put("manager","java.lang.Boolean");
		htblColNameType.put("location","java.awt.Polygon");

//		
//		Hashtable<String,String> htblColNameType1 = new Hashtable<String,String>();
//		htblColNameType1.put("name","java.lang.String");
//		htblColNameType1.put("city","java.lang.String");
//		htblColNameType1.put("id","java.lang.Integer");


////
//		htblColNameValue1.put("location", po);

//		Hashtable<String,Object> htblColNameValue2 = new Hashtable<String,Object>();
//		htblColNameValue2.put("name","salma");
//		htblColNameValue2.put("city","zamalek");
//		htblColNameValue2.put("id",839);

//		try {
//			a.createTable("test", "name", htblColNameType);
//		}
//		catch(DBAppException e) {
//			System.out.println(e.getMessage());
//		}
//		
//		try {
//			a.deleteFromTable("test", htblColNameValue1);
//		}
//		catch(DBAppException e) {
//			System.out.println(e.getMessage());
//		}
		
//		try {
//			a.updateTable("First Test", "11", htblColNameValue2);
//		}
//		catch(DBAppException e) {
//			System.out.println(e.getMessage());
//		}
	
//		try {
//			a.deleteFromTable("Table I", htblColNameValue1);
//		}
//		catch(DBAppException e) {
//			System.out.println(e.getMessage());
//		}
//		Hashtable<String,Object> htblColNameValue1 = new Hashtable<String,Object>();
//		htblColNameValue1.put("name","apple");
//		htblColNameValue1.put("salary",10.1);
//		htblColNameValue1.put("id", 8);
//		htblColNameValue1.put("manager", true);
//				
//		for(int j = 0;j < 50; j+=2) {
//		Polygon po = new Polygon();
//			po.addPoint(0, 3+j);
//			po.addPoint(23+j, 0);
//			po.addPoint(20, 33+j);
//			po.addPoint(3, 30+j);
//			htblColNameValue1.put("location", po);
//
//			try {
//				a.insertIntoTable("test", htblColNameValue1);
//			}
//			catch(DBAppException e) {
//				System.out.println(e.getMessage());
//			}
//			
//		}
//		

//		a.createBTreeIndex("test", "salary");
//		a.createRTreeIndex("test", "location");
		
		System.out.println("-------------Table 1-------------");
		Table t = a.tables.get(0);
		for(int i = 1; i <= t.getNoOfPages(); i++) {
			System.out.print("Page " + i);
			Page p = Page.loadPage(t.getTableName(), i);
			System.out.print(" ,size " + p.size() + "\n");
			p.print();
		}
		
//		System.out.println("\n\nR Tree as a primary index:");
//		@SuppressWarnings("rawtypes")
		System.out.println("--- --- --- --- ---");
		RTree rt =Index.loadRTIndex("test", "location");
				System.out.println(rt.toString());
//		RRef rr = rt.search(po);
//		System.out.println(ParsePolygon.getArea(po) + " indices:");
//		System.out.println("\n" + rr.toString());
//		
//		System.out.println("\n\nB+ Tree as a secondary index:");
//		BPTree bt =Index.loadBTIndex("Table P", "country");
//		System.out.println(bt.toString());
//		Ref r = bt.search("usa");
//		System.out.println("usa indices:");
//		System.out.println("\n" + r.toString());
//		
//		System.out.println("-------------Table 2-------------");
//		t = a.tables.get(1);
//		
//		for(int i = 1; i <= t.getNoOfPages(); i++) {
//			System.out.print("Page " + i);
//			Page p = Page.loadPage(t.getTableName(), i);
//			System.out.print(" ,size " + p.size() + "\n");
//			p.print();
//		}
		
//		System.out.println("\n\nB+ Tree as a primary index:");
//		BPTree bt = Index.loadBTIndex("test", "name");
	//	System.out.println(bt.toString());
//		r = bt.search(839);
//		System.out.println("839 indices:");
//		System.out.println("\n" + r.toString());
//		
//		System.out.println("\n\nB+ Tree as a secondary index:");
//		bt =Index.loadBTIndex("Table I", "city");
//		System.out.println(bt.toString());
//		r = bt.search("sheraton");
//		System.out.println("sheraton indices:");
//		System.out.println("\n" + r.toString());
		
//
		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[2];

		arrSQLTerms[0] = new SQLTerm("test", "location", "<", "(0,13),(33,0),(20,43),(3,40)");		
		arrSQLTerms[1] = new SQLTerm("test", "salary", ">", 8.5);

		String[] strarrOperators = new String[1];
		strarrOperators[0] = "and";
		a.selectFromTable(arrSQLTerms, strarrOperators);
	}
}
