package rTree;

import java.awt.Polygon;
import java.io.Serializable;

public class RTreeLeafNode<T extends Polygon> extends RTreeNode<T> implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private RRef[] records;
	private RTreeLeafNode<T> next;
	
	public RTreeLeafNode(int n) 
	{
		super(n);
		keys = new Polygon[n];
		records = new RRef[n];
	}
	
	/**
	 * @return the next leaf node
	 */
	public RTreeLeafNode<T> getNext()
	{
		return this.next;
	}
	
	/**
	 * sets the next leaf node
	 * @param node the next leaf node
	 */
	public void setNext(RTreeLeafNode<T> node)
	{
		this.next = node;
	}
	
	/**
	 * @param index the index to find its record
	 * @return the reference of the queried index
	 */
	public RRef getRecord(int index) 
	{
		return records[index];
	}
	
	/**
	 * sets the record at the given index with the passed reference
	 * @param index the index to set the value at
	 * @param recordReference the reference to the record
	 */
	public void setRecord(int index, RRef recordReference) 
	{
		records[index] = recordReference;
	}

	/**
	 * @return the reference of the last record
	 */
	public RRef getFirstRecord()
	{
		return records[0];
	}

	/**
	 * @return the reference of the last record
	 */
	public RRef getLastRecord()
	{
		return records[numberOfKeys-1];
	}
	
	/**
	 * finds the minimum number of keys the current node must hold
	 */
	public int minKeys()
	{
		if(this.isRoot())
			return 1;
		return (order + 1) / 2;
	}
	
	/**
	 * insert the specified key associated with a given record reference in the R tree
	 */
	public PushUp<T> insert(T key, RRef recordReference, RTreeInnerNode<T> parent, int ptr)
	{
		int[] result = this.findIndex(key);
		if(result[1] == 1) {	//the key is duplicated
			if(!(this.records[result[0]] instanceof ROverflowPage)) {
				ROverflowPage page = new ROverflowPage();
				page.getRecords().add(this.getRecord(result[0]));	//add the already existing record
				page.getRecords().add(recordReference);		//add the new record
				this.records[result[0]] = page;			//update the old reference so that it now refers to the page we created
			}
			else {
				ROverflowPage page = (ROverflowPage) this.records[result[0]];
				while(page.getNext()!=null) {
					page = page.getNext();
				}
				if(page.getRecords().size()<page.getMaxSize()) {
					page.getRecords().add(recordReference);
				}
				else {
					ROverflowPage newPage = new ROverflowPage();
					page.setNext(newPage);
					newPage.getRecords().add(recordReference);
				}
			}
			return null;
		}
		if(this.isFull())
		{
			RTreeNode<T> newNode = this.split(key, recordReference);
			Polygon newKey = newNode.getFirstKey();
			return new PushUp<T>(newNode, newKey);
		}
		else
		{
			int index = 0;
			while (index < numberOfKeys && compareTo(getKey(index), key) <= 0)
				++index;
			this.insertAt(index, key, recordReference);
			return null;
		}
	}
	
	/**
	 * inserts the passed key associated with its record reference in the specified index
	 * @param index the index at which the key will be inserted
	 * @param key the key to be inserted
	 * @param recordReference the pointer to the record associated with the key
	 */
	private void insertAt(int index, Polygon key, RRef recordReference) 
	{
		for (int i = numberOfKeys - 1; i >= index; --i) 
		{
			this.setKey(i + 1, getKey(i));
			this.setRecord(i + 1, getRecord(i));
		}

		this.setKey(index, key);
		this.setRecord(index, recordReference);
		++numberOfKeys;
	}
	
	/**
	 * splits the current node
	 * @param key the new key that caused the split
	 * @param recordReference the reference of the new key
	 * @return the new node that results from the split
	 */
	public RTreeNode<T> split(T key, RRef recordReference) 
	{
		int keyIndex = this.findIndex(key)[0];
		int midIndex = numberOfKeys / 2;
		if((numberOfKeys & 1) == 1 && keyIndex > midIndex)	//split nodes evenly
			++midIndex;		

		
		int totalKeys = numberOfKeys + 1;
		//move keys to a new node
		RTreeLeafNode<T> newNode = new RTreeLeafNode<T>(order);
		for (int i = midIndex; i < totalKeys - 1; ++i) 
		{
			newNode.insertAt(i - midIndex, this.getKey(i), this.getRecord(i));
			numberOfKeys--;
		}
		
		//insert the new key
		if(keyIndex < totalKeys / 2)
			this.insertAt(keyIndex, key, recordReference);
		else
			newNode.insertAt(keyIndex - midIndex, key, recordReference);
		
		//set next pointers
		newNode.setNext(this.getNext());
		this.setNext(newNode);
		
		return newNode;
	}
	
	/**
	 * finds the index at which the passed key must be located 
	 * @param key the key to be checked for its location
	 * @return the expected index of the key
	 */
	public int[] findIndex(T key) 
	{
		for (int i = 0; i < numberOfKeys; ++i) 
		{
			int cmp = compareTo(getKey(i), key);
			if (cmp == 0) {
				return new int[] {i,1};
			}
			if (cmp > 0) 
				return new int[] {i,0};
		}
		return new int[] {numberOfKeys,0};
	}

	/**
	 * returns the record reference with the passed key and null if does not exist
	 */
	@Override
	public RRef search(T key) 
	{
		for(int i = 0; i < numberOfKeys; ++i) {
			if(compareTo(this.getKey(i), key) == 0)
				return this.getRecord(i);
		}
		return null;
	}
	
	/**
	 * delete the passed key from the R tree
	 */
	public boolean delete(T key, RTreeInnerNode<T> parent, int ptr) 
	{
		for(int i = 0; i < numberOfKeys; ++i)
			if(compareTo(keys[i], key) == 0)
			{
				this.deleteAt(i);
				if(i == 0 && ptr > 0)
				{
					//update key at parent
					parent.setKey(ptr - 1, this.getFirstKey());
				}
				//check that node has enough keys
				if(!this.isRoot() && numberOfKeys < this.minKeys())
				{
					//1.try to borrow
					if(borrow(parent, ptr))
						return true;
					//2.merge
					merge(parent, ptr);
				}
				return true;
			}
		return false;
	}
	
	/**
	 * delete a key at the specified index of the node
	 * @param index the index of the key to be deleted
	 */
	public void deleteAt(int index)
	{
		if(records[index] instanceof ROverflowPage) {
			ROverflowPage page = (ROverflowPage) records[index];
			page.setNext(null);
			page = null;
		}
		for(int i = index; i < numberOfKeys - 1; ++i)
		{
			keys[i] = keys[i+1];
			records[i] = records[i+1];
		}
		numberOfKeys--;
	}
	
	/**
	 * tries to borrow a key from the left or right sibling
	 * @param parent the parent of the current node
	 * @param ptr the index of the parent pointer that points to this node 
	 * @return true if borrow is done successfully and false otherwise
	 */
	public boolean borrow(RTreeInnerNode<T> parent, int ptr)
	{
		//check left sibling
		if(ptr > 0)
		{
			RTreeLeafNode<T> leftSibling = (RTreeLeafNode<T>) parent.getChild(ptr-1);
			if(leftSibling.numberOfKeys > leftSibling.minKeys())
			{
				this.insertAt(0, leftSibling.getLastKey(), leftSibling.getLastRecord());		
				leftSibling.deleteAt(leftSibling.numberOfKeys - 1);
				parent.setKey(ptr - 1, keys[0]);
				return true;
			}
		}
		
		//check right sibling
		if(ptr < parent.numberOfKeys)
		{
			RTreeLeafNode<T> rightSibling = (RTreeLeafNode<T>) parent.getChild(ptr+1);
			if(rightSibling.numberOfKeys > rightSibling.minKeys())
			{
				this.insertAt(numberOfKeys, rightSibling.getFirstKey(), rightSibling.getFirstRecord());
				rightSibling.deleteAt(0);
				parent.setKey(ptr, rightSibling.getFirstKey());
				return true;
			}
		}
		return false;
	}
	
	/**
	 * merges the current node with its left or right sibling
	 * @param parent the parent of the current node
	 * @param ptr the index of the parent pointer that points to this node 
	 */
	public void merge(RTreeInnerNode<T> parent, int ptr)
	{
		if(ptr > 0)
		{
			//merge with left
			RTreeLeafNode<T> leftSibling = (RTreeLeafNode<T>) parent.getChild(ptr-1);
			leftSibling.merge(this);
			parent.deleteAt(ptr-1);			
		}
		else
		{
			//merge with right
			RTreeLeafNode<T> rightSibling = (RTreeLeafNode<T>) parent.getChild(ptr+1);
			this.merge(rightSibling);
			parent.deleteAt(ptr);
		}
	}
	
	/**
	 * merge the current node with the specified node. The foreign node will be deleted
	 * @param foreignNode the node to be merged with the current node
	 */
	public void merge(RTreeLeafNode<T> foreignNode)
	{
		for(int i = 0; i < foreignNode.numberOfKeys; ++i)
			this.insertAt(numberOfKeys, foreignNode.getKey(i), foreignNode.getRecord(i));
		
		this.setNext(foreignNode.getNext());
	}
}
