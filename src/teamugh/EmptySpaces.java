package teamugh;

import java.io.IOException;

public class EmptySpaces {

	public static void shift(Table table, int[] first, int[] last) throws IOException {
		int pIndexF = first[0];
		int pIndexL = last[0];
		int shiftCount;
		if(pIndexF != pIndexL) {	//the updated records were not in the same page
			System.out.println("the updated records were not in the same page");
			//delete empty pages
			int difference = pIndexL - pIndexF;
			if(difference > 1) {
				System.out.println("deleting empty pages");
				int i = pIndexF+2;
				while(difference > 1) {
					Page p = Page.loadPage(table.getTableName(), i);
					if(p.isEmpty() || p.size() == 0) {
						table.removePage();
						Page.deletePage(table.getTableName(), i);
					}
					i++;
					difference--;
				}
				table.renamePages();
			}
			//fill the first page first
			if(pIndexF<table.getNoOfPages()-1) {
				Page p = Page.loadPage(table.getTableName(), (pIndexF+1));
				Page n = Page.loadPage(table.getTableName(), (pIndexF+2));
				shiftCount = p.getMaxRows() - p.size()-1;
				while(shiftCount>0 && !n.isEmpty()) {
					Object[] rec = n.get(0);
					n.remove(0);
					p.add(rec);
					shiftCount--;
				}
				p.savePage(table.getTableName(), (pIndexF+1));
				n.savePage(table.getTableName(), (pIndexF+2));
				pIndexF++;
			}
		}
		while(pIndexF<table.getNoOfPages()-1) {
			Page p = Page.loadPage(table.getTableName(), (pIndexF+1));
			Page n = Page.loadPage(table.getTableName(), (pIndexF+2));
			shiftCount = p.getMaxRows() - p.size();
			while(shiftCount>0 && !n.isEmpty()) {
				Object[] rec = n.get(0);
				n.remove(0);
				p.add(rec);
				shiftCount--;
			}
			p.savePage(table.getTableName(), (pIndexF+1));
			n.savePage(table.getTableName(), (pIndexF+2));			
			pIndexF++;
		}
		int pages = table.getNoOfPages();
		for(int i = 1;i<pages+1;i++) {
			Page p = Page.loadPage(table.getTableName(), i);
			if(p.isEmpty()) {
				table.removePage();
				Page.deletePage(table.getTableName(), i);
			}
		}
		table.renamePages();
		table.saveTable();
	}
}