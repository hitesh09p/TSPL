package com.compareXML;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import org.custommonkey.xmlunit.DetailedDiff;
import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.Difference;
import org.custommonkey.xmlunit.XMLUnit;
import org.xml.sax.SAXException;
public class XMLComparator {
	public static void main(String args[]) throws FileNotFoundException, 
    SAXException, IOException {
		
		FileInputStream fis1 = new FileInputStream("C:\\Users\\bigdatagurukul\\Desktop\\source1.xml"); 
		FileInputStream fis2 = new FileInputStream("C:\\Users\\bigdatagurukul\\Desktop\\source2.xml");
		
		BufferedReader source = new BufferedReader(new InputStreamReader(fis1));
		BufferedReader target = new BufferedReader(new InputStreamReader(fis2));

		XMLUnit.setIgnoreWhitespace(true);
		
	 List differences = compareXML(source, target);
	 
	 printDifferences(differences);
	 
}
	public static List compareXML(Reader source, Reader target) throws SAXException, IOException{

		 Diff xmlDiff = new Diff(source, target);
		 DetailedDiff detailXmlDiff = new DetailedDiff(xmlDiff);
		 return detailXmlDiff.getAllDifferences();
	}
	
	public static void printDifferences(List differences){
		int totalDifferences = differences.size();
        System.out.println("===============================");
        System.out.println("Total differences : " + totalDifferences);
        System.out.println("================================");
    
        for (Object object : differences) {
            Difference difference = (Difference)object;
            System.out.println(difference);
            
        }
	}
	
}
