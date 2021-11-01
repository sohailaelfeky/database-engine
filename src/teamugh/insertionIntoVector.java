package teamugh;

import java.awt.Polygon;
import java.util.Vector;

public class insertionIntoVector {
	
	public static int helper(Vector<Object[]> rows, Object[] input, int l, int r, int ckIndex, String type) {
		//System.out.println(l+ " "+r);
		if (r==l) {
			//System.out.println("**********************");
			if(compareInput(input[ckIndex],rows.get(r)[ckIndex], type) == -1)
				return r;
			if(compareInput(input[ckIndex],rows.get(r)[ckIndex], type) == 1)
				return r+1;
			return r;
       	}
		if (l-r == 1) {
      		 return l;
      	}
        if (r>l) {
            int midRecordIndex = l + (r - l)/2;
            Object[] mid = rows.get(midRecordIndex);
            //Object[] min = rows.get(l);
            
            if(compareInput(input[ckIndex],mid[ckIndex], type) == 0) {
            	//System.out.println(input[ckIndex] + " equals " + mid[ckIndex]);
            	return midRecordIndex;
            }
            if(compareInput(input[ckIndex],mid[ckIndex], type) == -1) {	//input is less than the middle
            	//System.out.println(input[ckIndex] + " less than " + mid[ckIndex]);
            	return helper(rows, input, l, midRecordIndex-1, ckIndex, type);
            }
            if(compareInput(input[ckIndex],mid[ckIndex], type) == 1) {	//input is more than the mid
            	//System.out.println(input[ckIndex] + " more than " + mid[ckIndex]);
            	return helper(rows, input, midRecordIndex+1, r, ckIndex, type);
            }
         }
        return -1;
	}
	public static int compareInput(Object i, Object m, String type) {
		int compResult = -1;
		switch(type) {
			 case "java.lang.Integer":
	         	Integer i1 = (Integer) i;
		        Integer i2 = (Integer) m;
		        compResult = i1.compareTo(i2);
		        break;
	         case "java.lang.String":
	         	String s1 = (String) i;
	            String s2 = (String) m;
		        compResult = s1.compareTo(s2);
		        break;
	         case "java.lang.Double":
	         	Double d1 = (Double) i;
	            Double d2 = (Double) m;
		        compResult = d1.compareTo(d2);
		        break;
	         case "java.awt.Polygon": 
	         	Polygon p1 = (Polygon) i;
	         	Polygon p2 = (Polygon) m;
	     		Double d = (double) (p1.getBounds().getSize().width * p1.getBounds().getSize().height);
	     		Double c = (double) (p2.getBounds().getSize().width * p2.getBounds().getSize().height);
		        compResult = d.compareTo(c);
		        break;
	      	case "java.lang.Boolean":  
	         	Boolean b1 = (Boolean) i;
	         	Boolean b2 = (Boolean) m;
	         	compResult = b1.compareTo(b2);
	            break;
	         case "java.util.Date" :  
	         	Integer d3 = (Integer) i;
	         	Integer d4 = (Integer) m;
		        compResult = d3.compareTo(d4);
		        break;
	         default:;
		}
		if (compResult == 0) {		//input is equal to the existing element
        	return 0;
        }
        if (compResult > 0) {		//input is more than the existing element
           return 1;
        }
        return -1;		//input is less than the existing element
	}
	/*public static int helper(Vector<Object []> vec, Object[] x, int l, int r, int ck, Object type) throws DBAppException {
		//System.out.println(r + " " + l);
		String t = (String) type;
		int compResult;
		if (r==l) {
    		return r;
       	}
       	 if (l-r == 1) {
       		 return r+1;
       	 }
        if (r>l) {
            int mid = l + (r - l)/2; 
            // If the element is present at the  
            // middle itself 
            Object[] o = vec.get(mid);
            switch(t) {					//compare the ck of the middle element of the vector with the input
            case "java.lang.Integer":
            	Integer i1 =((Integer) x[ck]);
	            Integer i2 = (Integer) o[ck];
	            compResult = i1.compareTo(i2);
	            break;
            case "java.lang.String":
            	String s1 =((String) x[ck]);
                String s2 = (String) o[ck];
	            compResult = s1.compareTo(s2);
	            break;
            case "java.lang.Double":
            	Double d1 =((Double) x[ck]);
                Double d2 = (Double) o[ck];
	            compResult = d1.compareTo(d2);
	            break;
            case "java.awt.Polygon": 
            	Polygon p1 = (Polygon) x[ck];
            	Polygon p2 = (Polygon) o[ck];
        		Double d = (double) (p1.getBounds().getSize().width * p1.getBounds().getSize().height);
        		Double c = (double) (p2.getBounds().getSize().width * p2.getBounds().getSize().height);
	            compResult = d.compareTo(c);
	            break;
         	case "java.lang.Boolean":  
         		Boolean b1 =((Boolean) x[ck]);
         		Boolean b2 = (Boolean) o[ck];
         		compResult = b1.compareTo(b2);
         		break;
            case "java.util.Date" :  
            	Integer d3 =((Integer) x[ck]);
            	Integer d4 = (Integer) o[ck];
	            compResult = d3.compareTo(d4);
	            break;
            default:
            	throw new DBAppException();
         }
            if (compResult == 0) {
            	return mid;
            }
            // If element is smaller than mid, then  
            // it can only be present in left subarray
            if (compResult > 0) {
               return helper(vec, x, l, mid-1, ck, type); 
            }
            // Else the element can only be present 
            // in right subarray 
            if (compResult < 0) {
            	return helper(vec, x, mid+1, r, ck, type);  
            }
        } 
        // We reach here when element is not present 
        //  in array 
        return -1; 
	}
	/*public static void main(String[] arg) {
		Object j = "java.lang.Double";
		Vector<Object []> vec = new Vector<Object []>();
		Object[] x = new Object[3];
		x[0] = 1;
		x[1] = "yes";
		x[2] = 2.3;
		
		Object[] y = new Object[3];
		y[0] = 2;
		y[1] = "bye";
		y[2] = 3.6;
		
		Object[] z = new Object[3];
		z[0] = 3;
		z[1] = "no";
		z[2] = 2.4;
		
		Object[] m = new Object[3];
		m[0] = 4;
		m[1] = "no";
		m[2] = 2.4;
		
		Object[] k = new Object[3];
		k[0] = 4;
		k[1] = "no";
		k[2] = 9.6;
		
		InsertionCheckers.insertion(vec, x , 0, vec.size(),  2, j);
		for(int i=0; i< vec.size(); i++){ 
			Object[] objs = vec.get(i);
			for (Object o : objs) {
			    System.out.print(o + " ");
			}
			System.out.print(",");
			}
		System.out.println("");
		
		InsertionCheckers.insertion(vec, k , 0, vec.size(),  2, j);
		for(int i=0; i< vec.size(); i++){ 
			Object[] objs = vec.get(i);
			for (Object o : objs) {
			    System.out.print(o + " ");
			}
			System.out.print(",");
			}
		System.out.println("");

		InsertionCheckers.insertion(vec, y , 0, vec.size(),  2, j);

		
		for(int i=0; i< vec.size(); i++){ 
		Object[] objs = vec.get(i);
		for (Object o : objs) {
		    System.out.print(o + " ");
		}
		System.out.print(",");
		}
		System.out.println("");

		InsertionCheckers.insertion(vec, z , 0, vec.size(),  2, j);
		for(int i=0; i< vec.size(); i++){ 
			Object[] objs = vec.get(i);
			for (Object o : objs) {
			    System.out.print(o + " ");
			}
			System.out.print(",");

			}
		
		System.out.println("");

		InsertionCheckers.insertion(vec, m , 0, vec.size(),  2 ,j);
		for(int i=0; i< vec.size(); i++){ 
			Object[] objs = vec.get(i);
			for (Object o : objs) {
			    System.out.print(o + " ");
			}
			System.out.print(",");

			}	
		
	}*/
}
