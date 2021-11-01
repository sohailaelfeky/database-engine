package teamugh;

import java.awt.Polygon;

public class ParsePolygon {

	public static Polygon returnPolygon(String s) {
		String[] points = s.split("\\),\\(");
		points[0] = points[0].substring(1, points[0].length());
		String[] s2 = points[points.length-1].split("\\)");
		points[points.length-1] = s2[0];
		Polygon p = new Polygon();
		
		for(int i = 0; i < points.length; i++) {
		System.out.println(points[i]);
	}
		for(int i = 0; i < points.length; i++) {
			String[] coordinates = points[i].split(",");
			int x = Integer.parseInt(coordinates[0]);
			int y = Integer.parseInt(coordinates[1]);
			p.addPoint(x, y);
		}
		return p;
	}
	
	public static double getArea(Object o) {
		Polygon p = (Polygon) o;
		return getArea(p);
	}
	
	public static double getArea(Polygon p) {
		double area = p.getBounds().getWidth() * p.getBounds().getHeight();
		return area;
	}
	
	public static int compareByArea(Object x, Object y) {		//this uses areas
		Polygon a = (Polygon) x;
		Polygon b = (Polygon) y;
 		return compareByArea(a, b);
	}
	
	//overloading
	public static int compareByArea(Polygon a, Polygon b) {		//this uses areas
 		Double d = (double) (a.getBounds().getSize().width * a.getBounds().getSize().height);
 		Double c = (double) (b.getBounds().getSize().width * b.getBounds().getSize().height);
        int compResult = d.compareTo(c);

        if (compResult == 0) {		//area of a = area of b
        	return 0;
        }
        if (compResult > 0) {		//area of a > area of b
           return 1;
        }
        return -1;		//area of a < area of b
	}
	

	public static boolean compareByCoordinates(Object x, Object y) {		//this uses coordinates
		Polygon a = (Polygon) x;
		Polygon b = (Polygon) y;
		return compareByCoordinates(a, b);
	}
	//overloading
	public static boolean compareByCoordinates(Polygon a, Polygon b) {		//this uses coordinates
		int[] ax = a.xpoints;
		int[] ay = a.ypoints;
		int[] bx = b.xpoints;
		int[] by = b.ypoints;
		
		if(ax.length != bx.length) {
			return false;
		}
		
		if(ay.length != by.length) {
			return false;
		}
		
		for(int i=0;i<ax.length;i++) {
			if(ax[i] != bx[i])
				return false;
		}
		
		for(int i=0;i<ay.length;i++) {
			if(ay[i] != by[i])
				return false;
		}
		return true;
	}
	
	public static String toString(Object o) {
		Polygon p = (Polygon) o;
		return toString(p);
	}
	
	public static String toString(Polygon p) {
		int[] x = p.xpoints;
		int[] y = p.ypoints;
		String out = "";
		for(int i=0;i<x.length && i<y.length;i++) {
			out+= "(" + x[i] + "," + y[i] + ")";
			if(i!=x.length-1)
				out+=",";
		}
		return out;
	}
}