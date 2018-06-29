package com.compareXML;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.custommonkey.xmlunit.DetailedDiff;
import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.XMLUnit;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import junit.framework.Assert;

public class xmlDiff {
	public static void main(String[] args) throws Exception {

		/*
		 * String arr1= "<?xml>, <class>, <student>"; String arr2=
		 * "<?xml>, <class>, <stud2>"; String result =
		 * "<abc             attr=\"value1\"                title=\"something\">            </abc>"
		 * ; // will be ok
		 * assertXMLEquals("<abc attr=\"value1\" title=\"something\"></abc>", result);
		 * // XMLUnit.setIgnoreWhitespace(true); // assertXMLEquals(arr1, arr2); }
		 * 
		 * public static void assertXMLEquals(String expectedXML, String actualXML)
		 * throws Exception { XMLUnit.setIgnoreWhitespace(true);
		 * XMLUnit.setIgnoreAttributeOrder(true);
		 * 
		 * DetailedDiff diff = new DetailedDiff(XMLUnit.compareXML(expectedXML,
		 * actualXML));
		 * 
		 * List<?> allDifferences = diff.getAllDifferences();
		 * Assert.assertEquals("Differences found: "+ diff.toString(), 0,
		 * allDifferences.size()); }
		 */

		// [1,2,3,4,5]
		List<String> list1 = Arrays.asList("<?xml>", "<class>", "<student>");
		List<String> list2 = Arrays.asList("<?xml>", "<class>", "<stud2>");
		// Prepare a union
		ArrayList<String> union = new ArrayList<String>(list1);
		union.addAll(list2);
		// System.out.println(union);
		// Prepare an intersection
		ArrayList<String> intersection = new ArrayList<String>(list1);
		intersection.retainAll(list2);
		// System.out.println(intersection);
		// Subtract the intersection from the union
		union.retainAll(intersection);
		// union.remove(intersection);
		// System.out.println(union);
		// Print the result
		for (String n : union) {
			System.out.println(n);
		}
	}

}
