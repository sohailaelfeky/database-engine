package rTree;

import java.io.Serializable;

import bPlusTree.Reference;

public class RRef extends Reference implements Serializable{
	
	/**
	 * This class represents a pointer to the record. It is used at the leaves of the R tree 
	 */
	private static final long serialVersionUID = 1L;
	private int pageNo, indexInPage;
	private ROverflowPage overflowPage;
	
	public RRef(int pageNo, int indexInPage)
	{
		this.pageNo = pageNo;
		this.indexInPage = indexInPage;
	}
	
	public RRef()
	{
		
	}
	
	/**
	 * @return the page at which the record is saved on the hard disk
	 */
	public int getPage()
	{
		return pageNo;
	}
	
	/**
	 * @return the index at which the record is saved in the page
	 */
	public int getIndexInPage()
	{
		return indexInPage;
	}
	
	public ROverflowPage getOverflowPage() {
		return this.overflowPage;
	}
}
