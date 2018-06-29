package com.compareXML;



 
import java.io.File;
 
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;
import java.util.List;
import java.util.ArrayList;

import java.io.Serializable;
import java.util.EventListener;
import java.util.HashMap;
import java.util.Hashtable;
 

public class Compxml {

	public static void main(String[] args) {
		//System.out.println("hello");
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
		   // NodeList nList1 = doc1.getElementsByTagName("*");
//		    System.out.println(nList+"\n"+nList1+"\n"+"----------------------------");
//	        HashMap<Node, Node> inhmap =             new HashMap<Node, Node>();
//	        HashMap<Node, inhmap> hmap =             new HashMap<Node, inhmap>();
		    for (int temp = 0; temp < nList.getLength(); temp++) {
		    	
		    	Node nNode = nList.item(temp);
		    	for (int temp1 = 0; temp1 < nList1.getLength(); temp1++) {	
		    		
		    		
		        Node nNode1 = nList1.item(temp1);
		        if(nNode.isEqualNode(nNode1)==false) {		        	
		        	if(nNode.getNodeName().equals(nNode1.getNodeName()))
		        	{
//		        		System.out.println("nodename same");
		        		System.out.println("same node different attributes:" + nNode1.getNodeName());
		        		
		        	
//		        		Element Element = doc.createElement(nNode1.getNodeName());
//		        		Element.appendChild(nNode1);
		        		for(int i = 0; i<nNode.getAttributes().getLength();i++ ){
		        			for(int j = 0; j<nNode1.getAttributes().getLength();j++ ){		        				
		        			if(nNode.getAttributes().item(i).equals(nNode1.getAttributes().item(j)))
		        			{
		        				continue;
		        			}
		        			else
		        			{
		        				String attrname= nNode1.getAttributes().item(j).toString().substring(0, nNode1.getAttributes().item(j).toString().indexOf("="));
		        				System.out.println("node 1 attributes:" + nNode1.getAttributes().item(j).toString().substring(0, nNode1.getAttributes().item(j).toString().indexOf("=")));
		        				
		        				//System.out.println(nNode1);
		        				
		        				TransformerFactory transformerFactory =  TransformerFactory.newInstance();
		    			        Transformer transformer = transformerFactory.newTransformer();
		    			        
		    			            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		    			            DOMSource source = new DOMSource(nNode1);

		    			        StreamResult result =  new StreamResult(new File("B:\\testing.xml"));
		    			        transformer.transform(source, result);
		        				
		        				
		        				
		        			}
		        			
		        		}
		        			break;
		        		}
		        		
		        	}
		        	else ///nodename not same
		        	{			
		        		continue;
		        		
		        	}
//		        
//		        break;
		        }
		        else {
		        	
		        	System.out.println("equal:\n" + nNode1.getNodeName());
		        	for(int i = 0; i<nNode1.getAttributes().getLength();i++ ){
		        		System.out.println("attributes:" + nNode1.getAttributes().item(i));
		        		}
		        }
		       
		    	}
		    	  
		    }
//		   
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
		
		
		
		
		
		
		
		}
	}
	
	


