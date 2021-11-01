package teamugh;

import java.awt.Polygon;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Properties;
import java.util.Vector;

public class Page extends Vector<Object[]> implements Serializable {

	private static final long serialVersionUID = 5390161204514601996L;
	private int maxRows;
	
	public Page() {
		try {
			maxRows = getMaximumRowsCountinPage();
		}
		catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public static int getMaximumRowsCountinPage() throws IOException {
		FileReader fileReader = new FileReader("config/DBApp.properties");
		Properties p = new Properties();
		p.load(fileReader);
		return Integer.parseInt(p.getProperty("MaximumRowsCountinPage"));
	}
	
	public Vector<Object[]> getRows() {
		return this;
	}
	
	public int getMaxRows() {
		return this.maxRows;
	}
	
	public void savePage(String strTableName, int pageNumber) throws IOException {
		try {
			File pa = new File("data/" + strTableName + "/page_" + pageNumber + ".ser");
			if(!pa.exists()) {
				pa.createNewFile();
			}
	        FileOutputStream fileOut = new FileOutputStream(pa);
	        ObjectOutputStream out = new ObjectOutputStream(fileOut);
	        out.writeObject(this);
	 		out.close();
	 		fileOut.close();
	        //System.out.println("Page serialized successfully!");
	      } catch (IOException i) {
	         i.printStackTrace();
	      }
	}
	
	public static Page loadPage(String strTableName, int pageNumber) {
		try {
	        FileInputStream fileIn = new FileInputStream("data/" + strTableName + "/" + "page_" + pageNumber + ".ser");
	        ObjectInputStream in = new ObjectInputStream(fileIn);
	        Page e = (Page) in.readObject();
	 		in.close();
	 		fileIn.close();
	 		return e;
	    }
		catch (IOException i) {
	         i.printStackTrace();
	    }
		catch (ClassNotFoundException c) {
	         System.out.println("Page not found");
	         c.printStackTrace();
	    }
		return null;
	}
	
	public static void deletePage(String strTableName, int pageNumber) {
		File pa = new File("data/" + strTableName + "/page_" + pageNumber + ".ser");
		if(pa.exists()) {
			pa.delete();
		}
	}
	
	public void print() {
		for(int i = 0;i<this.size();i++) {
			Object[] x = this.get(i);
			int j=0;
			while(j<x.length-1) {
				if(x[j] instanceof Polygon) {
					System.out.print(ParsePolygon.toString(x[j]) + ",");
				}
				else
					System.out.print(x[j] + ",");
				j++;
			}
			System.out.print(x[j]+"\n");
		}
	}
}