package com.compareXML;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
public class mergeXML {
	public static void main(String[] args) throws Exception {
        
		// Creates file to write to
		Writer output = null;
		output = new BufferedWriter(new FileWriter("C:\\Users\\bigdatagurukul\\Desktop\\merged.xml"));
		String newline = System.getProperty("line.separator");
		 ArrayList <String> addstr1 = new ArrayList <String>(); 
		 ArrayList <String> addstr2 = new ArrayList <String>();
		//output.write("<class>");
		 
		// Read in xml file 1
		FileInputStream in = new FileInputStream("C:\\Users\\bigdatagurukul\\Downloads\\source2.xml");
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		         
		while ((strLine = br.readLine()) != null) {
		         
	     addstr1.add(strLine);
		             
		//output.write(newline);
		//output.write(strLine);
		//System.out.println(strLine);
		}
//System.out.println(addstr1);
		         
		// Read in xml file 2
		FileInputStream in2 = new FileInputStream("C:\\Users\\bigdatagurukul\\Downloads\\source3.xml");
		BufferedReader br2 = new BufferedReader(new InputStreamReader(in2));
		String strLine2;
		         
		while ((strLine2 = br2.readLine()) != null) {
		                      
		//output.write(strLine2);
		//output.write(newline);
		//System.out.println(strLine2);
			addstr2.add(strLine2);
		}
		
		//System.out.println(addstr2);
		
		/*for (int i = 0; i <= addstr1.size(); i++)
		{*/
			
			for (int j = 0; j < addstr2.size(); j++)
			{
				
					//System.out.println(addstr2.get(j));
					//String s = addstr1.get(i).toString();
					String s1 = addstr2.get(j);
					//System.out.println(s);
					System.out.println(s1);
					
					
					//String str1 = s.substring(s.indexOf("<")+1, s.indexOf(" "));
				    //String str2 = s1.substring(s1.indexOf("<")+1, s1.indexOf(" "));
				    
				    
				    //System.out.println(str2);
				    /*if(!str1.equals(str2))
					{  
				    	System.out.println(str1 +"\n"+ str2);
					
					}*/
			}	
			

			
		//}
		
		
     	//output.write("</class>");
	
/*		Set<String> union = new HashSet<String>(addstr1);
        union.addAll(addstr2);
      System.out.println(union);
       // Prepare an intersection
        Set<String> intersection = new HashSet<String>(addstr1);
        intersection.retainAll(addstr2);
     //   System.out.println(intersection);
        // Subtract the intersection from the union
        //union.remove(intersection);
        // Print the result
        for (String n : union) {
            output.write(n);
            output.write(newline);
        }  */                         
		output.close();
		         
		System.out.println("Merge Complete");
		         
		}
	
}
