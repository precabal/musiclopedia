package com.precabal.musiclopedia

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;

public class ReadXMLFile {

  public static void main(String argv[]) {

    try {

	File xmlFile = new File("/home/ubuntu/discogs_20150901_artists.xml.gz");
	DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
	DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
	Document document = dBuilder.parse(xmlFile);
			
	document.getDocumentElement().normalize();

	System.out.println("Root element :" + document.getDocumentElement().getNodeName());
			
	NodeList nodeList = document.getElementsByTagName("staff");
			
	System.out.println("----------------------------");

	for (int nodeIndex = 0; nodeIndex < nodeList.getLength(); nodeIndex++) {

		Node node = nodeList.item(nodeIndex);
				
		System.out.println("\nCurrent Element :" + node.getNodeName());
				
		if (node.getNodeType() == Node.ELEMENT_NODE) {

			Element element = (Element) node;

			System.out.println("Staff id : " + element.getAttribute("id"));
			System.out.println("First Name : " + element.getElementsByTagName("firstname").item(0).getTextContent());
			System.out.println("Last Name : " + element.getElementsByTagName("lastname").item(0).getTextContent());
			System.out.println("Nick Name : " + element.getElementsByTagName("nickname").item(0).getTextContent());
			System.out.println("Salary : " + element.getElementsByTagName("salary").item(0).getTextContent());

		}
	}
    } catch (Exception e) {
	e.printStackTrace();
    }
  }

}