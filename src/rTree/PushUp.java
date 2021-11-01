package rTree;

import java.awt.Polygon;

public class PushUp<T extends Polygon> {

	/**
	 * This class is used for push keys up to the inner nodes in case
	 * of splitting at a lower level
	 */
	RTreeNode<T> newNode;
	Polygon key;
	
	public PushUp(RTreeNode<T> newNode, Polygon key)
	{
		this.newNode = newNode;
		this.key = key;
	}
}