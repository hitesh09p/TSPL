package com.compareXML;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import javax.swing.text.BadLocationException;
import javax.swing.text.Document;
import javax.swing.text.rtf.RTFEditorKit;


public class rtfToJson {

	public static void main(String[] args)throws IOException, BadLocationException {
		// TODO Auto-generated method stub
		RTFEditorKit rtf = new RTFEditorKit();
	    Document doc = rtf.createDefaultDocument();

	    FileInputStream fis = new FileInputStream("C:\\Users\\bigdatagurukul\\Downloads\\SampleINCData.rtf");
	    InputStreamReader i =new InputStreamReader(fis,"UTF-8");
	    rtf.read(i,doc,0);
	   // System.out.println(doc.getText(0,doc.getLength()));
	    String doc1 = doc.getText(0,doc.getLength());

	          
	    try{    
	           FileWriter fw=new FileWriter("B:\\Sample INC Data.json");    
	           fw.write(doc1);    
	           fw.close();    
	          }catch(Exception e)
	    {
	        	  System.out.println(e);
	        	  }    
	          System.out.println("Success...");    
	     }    
            

	}


