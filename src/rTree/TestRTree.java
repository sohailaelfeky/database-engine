package rTree;

import java.awt.Polygon;

public class TestRTree {

	public static void main(String[] args) 
	{
		RTree<Polygon> tree = new RTree<Polygon>(3);
		String f = "(10,20),(20,20),(20,40),(10,40)";
		String e = "(10,30),(20,30),(20,40),(50,60)";
		String g = "(0,30),(70,30),(70,100),(0,60)";
		String h = "(300,30),(70,30),(70,100),(300,60)";
		Polygon x = getPolygon(f);
		Polygon y = getPolygon(e);
		Polygon z = getPolygon(g);
		Polygon l = getPolygon(h);
		
		tree.insert(x, null);
		tree.insert(y, null);
		tree.insert(z, null);
		tree.insert(l, null);
		
		tree.toString();
	}	
	
	public static Polygon getPolygon(String s) {
		String[] points = s.split("\\),\\(");
		points[0] = points[0].substring(1,points[0].length());
		String[] s2 = points[points.length-1].split("\\)");
		points[points.length-1] = s2[0];
		Polygon p = new Polygon();
		for(int i=0;i<points.length;i++) {
			String[] coordinates = points[i].split(",");
			int x = Integer.parseInt(coordinates[0]);
			int y = Integer.parseInt(coordinates[1]);
			p.addPoint(x, y);
		}
		return p;
	}
}
