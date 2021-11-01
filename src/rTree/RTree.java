package rTree;

import java.awt.Polygon;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;

public class RTree<T extends Polygon> implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int order;
	private RTreeNode<T> root;
	
	/**
	 * Creates an empty R tree
	 * @param order the maximum number of keys in the nodes of the tree
	 */
	public RTree(int order) 
	{
		this.order = order;
		root = new RTreeLeafNode<T>(this.order);
		root.setRoot(true);
	}
	
	/**
	 * Inserts the specified key associated with the given record in the R tree
	 * @param key the key to be inserted
	 * @param recordReference the reference of the record associated with the key
	 */
	public void insert(T key, RRef recordReference)
	{
		PushUp<T> pushUp = root.insert(key, recordReference, null, -1);
		if(pushUp != null)
		{
			RTreeInnerNode<T> newRoot = new RTreeInnerNode<T>(order);
			newRoot.insertLeftAt(0, pushUp.key, root);
			newRoot.setChild(1, pushUp.newNode);
			root.setRoot(false);
			root = newRoot;
			root.setRoot(true);
		}
	}
	
	
	/**
	 * Looks up for the record that is associated with the specified key
	 * @param key the key to find its record
	 * @return the reference of the record associated with this key 
	 */
	public RRef search(T key)
	{
		return root.search(key);
	}
	
	/**
	 * Delete a key and its associated record from the tree.
	 * @param key the key to be deleted
	 * @return a boolean to indicate whether the key is successfully deleted or it was not in the tree
	 */
	public boolean delete(T key)
	{
		boolean done = root.delete(key, null, -1);
		//go down and find the new root in case the old root is deleted
		while(root instanceof RTreeInnerNode && !root.isRoot())
			root = ((RTreeInnerNode<T>) root).getFirstChild();
		return done;
	}
	
	/**
	 * Returns a string representation of the R tree.
	 */
	public String toString()
	{	
		
		//	<For Testing>
		// node :  (id)[k1|k2|k3|k4]{P1,P2,P3,}
		String s = "";
		Queue<RTreeNode<T>> cur = new LinkedList<RTreeNode<T>>(), next;
		cur.add(root);
		while(!cur.isEmpty())
		{
			next = new LinkedList<RTreeNode<T>>();
			while(!cur.isEmpty())
			{
				RTreeNode<T> curNode = cur.remove();
				System.out.print(curNode);
				if(curNode instanceof RTreeLeafNode)
					System.out.print("->");
				else
				{
					System.out.print("{");
					RTreeInnerNode<T> parent = (RTreeInnerNode<T>) curNode;
					for(int i = 0; i <= parent.numberOfKeys; ++i)
					{
						System.out.print(parent.getChild(i).index+",");
						next.add(parent.getChild(i));
					}
					System.out.print("} ");
				}
				
			}
			System.out.println();
			cur = next;
		}	
		//	</For Testing>
		return s;
	}
	
	public RTreeNode<T> root() {
		return root;
	}
}
