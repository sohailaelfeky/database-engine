package bPlusTree;

import java.io.Serializable;

public class Ref extends Reference implements Serializable{
	
	/**
	 * This class represents a pointer to the record. It is used at the leaves of the B+ tree 
	 */
	private static final long serialVersionUID = 1L;
	private int pageNo, indexInPage;
	private OverflowPage overflowPage;
	
	public Ref(int pageNo, int indexInPage)
	{
		this.pageNo = pageNo;
		this.indexInPage = indexInPage;
	}
	
	public Ref()
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
	
	public OverflowPage getOverflowPage() {
		return this.overflowPage;
	}
}
