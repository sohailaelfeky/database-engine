package rTree;

import java.awt.Polygon;
import java.io.Serializable;

import teamugh.ParsePolygon;

public abstract class RTreeNode<T extends Polygon> implements Serializable{
	
	/**
	 * Abstract class that collects the common functionalities of the inner and leaf nodes
	 */
	private static final long serialVersionUID = 1L;
	protected Polygon[] keys;
	protected int numberOfKeys;
	protected int order;
	protected int index;		//for printing the tree
	private boolean isRoot;
	private static int nextIdx = 0;

	public RTreeNode(int order) 
	{
		index = nextIdx++;
		numberOfKeys = 0;
		this.order = order;
	}
	
	public int getNumberOfKeys() {
		return numberOfKeys;
	}
	
	/**
	 * @return a boolean indicating whether this node is the root of the R tree
	 */
	public boolean isRoot()
	{
		return isRoot;
	}
	
	/**
	 * set this node to be a root or unset it if it is a root
	 * @param isRoot the setting of the node
	 */
	public void setRoot(boolean isRoot)
	{
		this.isRoot = isRoot;
	}
	
	/**
	 * find the key at the specified index
	 * @param index the index at which the key is located
	 * @return the key which is located at the specified index
	 */
	public Polygon getKey(int index) 
	{
		return keys[index];
	}

	/**
	 * sets the value of the key at the specified index
	 * @param index the index of the key to be set
	 * @param key the new value for the key
	 */
	public void setKey(int index, Polygon key) 
	{
		keys[index] = key;
	}
	
	/**
	 * @return a boolean whether this node is full or not
	 */
	public boolean isFull() 
	{
		return numberOfKeys == order;
	}
	
	/**
	 * @return the last key in this node
	 */
	public Polygon getLastKey()
	{
		return keys[numberOfKeys-1];
	}
	
	/**
	 * @return the first key in this node
	 */
	public Polygon getFirstKey()
	{
		return keys[0];
	}
	
	/**
	 * @return the minimum number of keys this node can hold
	 */
	public abstract int minKeys();

	/**
	 * insert a key with the associated record reference in the B+ tree
	 * @param key the key to be inserted
	 * @param recordReference a pointer to the record on the hard disk
	 * @param parent the parent of the current node
	 * @param ptr the index of the parent pointer that points to this node
	 * @return a key and a new node in case of a node splitting and null otherwise
	 */
	public abstract PushUp<T> insert(T key, RRef recordReference, RTreeInnerNode<T> parent, int ptr);
	
	public abstract RRef search(T key);

	/**
	 * delete a key from the B+ tree recursively
	 * @param key the key to be deleted from the R tree
	 * @param parent the parent of the current node
	 * @param ptr the index of the parent pointer that points to this node 
	 * @return true if this node was successfully deleted and false otherwise
	 */
	public abstract boolean delete(T key, RTreeInnerNode<T> parent, int ptr);
	
	/**
	 * A string represetation for the node
	 */
	public String toString()
	{		
		String s = "(" + index + ")";

		s += "[";
		for (int i = 0; i < order; i++)
		{
			String key = " ";
			if(i < numberOfKeys)
				key = toString(keys[i]);
			
			s+= key;
			if(i < order - 1)
				s += "|";
		}
		s += "]";
		return s;
	}
	private static String toString(Polygon p) {
		int[] x = p.xpoints;
		int[] y = p.ypoints;
		String out = "";
		for(int i=0;i<x.length && i<y.length;i++) {
			out+= "(" + x[i] + "," + y[i] + ")";
			if(i!=x.length-1)
				out+=",";
		}
		out += ", area:" + ParsePolygon.getArea(p);
		return out;
	}
	
	static int compareTo(Polygon a, Polygon b) {
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
}
