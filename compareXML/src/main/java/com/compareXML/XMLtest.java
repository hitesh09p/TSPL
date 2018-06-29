package com.compareXML;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;
import java.util.ArrayList;

public class XMLtest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		try {
			File fXmlFile = new File("C:\\Users\\bigdatagurukul\\Downloads\\source2.xml");
			File fXmlFile1 = new File("C:\\Users\\bigdatagurukul\\Downloads\\source3.xml");
		    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		    Document doc = dBuilder.parse(fXmlFile);
		    Document doc1 = dBuilder.parse(fXmlFile1);
		    doc.getDocumentElement().normalize();
		    doc1.getDocumentElement().normalize();
		    NodeList nList = doc.getElementsByTagName("*");

		    NodeList nList1 = doc1.getElementsByTagName("*");
		    
		    for (int temp = 0; temp < nList.getLength(); temp++) {
		    	for (int temp1 = 0; temp1 < nList1.getLength(); temp1++) {
		    	
		        Node nNode = nList.item(temp);
		        System.out.println(nNode.getNodeName());
		        Node nNode1 = nList1.item(temp1);
		        Element eElement1 = (Element) nNode; 
			      Element eElement2 = (Element) nNode1;
		        if (nNode.getNodeName() == nNode1.getNodeName()) {
		     // System.out.println(nNode1.getNodeName());
		      
		    //  System.out.println(eElement1.getAttributes().item(0));
		     // System.out.println(eElement2.getAttributes().item(0));

		      for(int i=0; i<eElement1.getAttributes().getLength();i++)
	            {
		    	  
		    	  for(int j=0; j<eElement2.getAttributes().getLength();j++)
		            {
		    		  
		    		  if (eElement1.getAttributes().item(i) != eElement2.getAttributes().item(j)) { 
		    		 // System.out.println(eElement1.getAttributes().item(i));
		    		//  System.out.println(eElement2.getAttributes().item(j));
		            }
		    		  
		    	}
		    	  
		    }
		    }
//		        else if (nList.item(temp).getNodeName() != nList.item(temp).getNodeName()) {
//		        	for(int i=0; i<eElement1.getAttributes().getLength();i++)
//		            {
//			    	  for(int j=0; j<eElement2.getAttributes().getLength();j++)
//			            {		  
//			    		 System.out.println(eElement1.getAttributes().item(i));
//			    		 System.out.println(eElement2.getAttributes().item(j));
//			            }
//			    		  
//			    	}
//			    	  
//			    }
		        	
		        }
		    	}
		
	
		        /*System.out.println("\nCurrent Element :" + nNode.getNodeName());
		        if (nNode.getNodeType() == Node.ELEMENT_NODE) {
		            Element eElement = (Element) nNode;
		            eElement.getAttributes();
		            
		            for(int i=0; i<eElement.getAttributes().getLength();i++)
		            {
		            System.out.println(eElement.getAttributes().item(i));
		            }
	        }
		    }*/
		
  
		/*    System.out.println("-----------Source3----------------");
		    NodeList nList1 = doc1.getElementsByTagName("*");
		    System.out.println("----------------------------");
		    
		    for (int temp = 0; temp < nList1.getLength(); temp++) {
		        Node nNode1 = nList1.item(temp);
		        System.out.println("\nCurrent Element :" + nNode1.getNodeName());
		        if (nNode1.getNodeType() == Node.ELEMENT_NODE) {
		            Element eElement = (Element) nNode1;
		            eElement.getAttributes();
		            
		            for(int i=0; i<eElement.getAttributes().getLength();i++)
		            {
		            System.out.println(eElement.getAttributes().item(i));
		            }
		        }
		    }*/
		
		}catch(

	Exception e)
	{
		e.printStackTrace();
	}

}

}
