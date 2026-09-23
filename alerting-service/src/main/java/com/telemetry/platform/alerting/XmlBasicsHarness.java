package com.telemetry.platform.alerting;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

public class XmlBasicsHarness {

    public static void main(String[] args) throws Exception {

        System.out.println("=== Section 1: building an XML document programmatically - the DOM tree model ===");
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.newDocument();

        Element root = document.createElement("alert");
        root.setAttribute("sensorId", "S-1001");
        document.appendChild(root);

        Element readingElement = document.createElement("reading");
        readingElement.setTextContent("71.2");
        root.appendChild(readingElement);

        Element notesElement = document.createElement("notes");
        notesElement.setTextContent("threshold breach: value > 70 & rising");
        root.appendChild(notesElement);

        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        StringWriter writer = new StringWriter();
        transformer.transform(new DOMSource(document), new StreamResult(writer));
        String xmlOutput = writer.toString();
        System.out.println(xmlOutput);
        System.out.println("(This is a tree model, same idea as Jackson's JsonNode tree: a Document root, Element nodes,");
        System.out.println("attributes, and text content - built programmatically via createElement()/appendChild(), never by");
        System.out.println("concatenating strings by hand. Look closely at the \"notes\" element's content below.)");

        System.out.println();
        System.out.println("=== Section 2: automatic escaping - why you never hand-build XML with string concatenation ===");
        System.out.println("Raw value written into the notes element: \"threshold breach: value > 70 & rising\"");
        System.out.println("(Look at the actual output above - the bare '&' character was automatically escaped to '&amp;'");
        System.out.println("by the Transformer. A naive string-concatenation approach - writing \"<notes>\" + rawValue +");
        System.out.println("\"</notes>\" directly - would have produced genuinely malformed XML the moment a value contained");
        System.out.println("an unescaped '&', '<', or '>'. This is the exact same class of bug Step 5 warned about for JSON,");
        System.out.println("just with XML's own escaping rules instead.)");

        System.out.println();
        System.out.println("=== Section 3: parsing XML back - walking the DOM tree to extract values ===");
        DocumentBuilder parser = factory.newDocumentBuilder();
        Document parsedDocument = parser.parse(new ByteArrayInputStream(xmlOutput.getBytes(StandardCharsets.UTF_8)));
        Element parsedRoot = parsedDocument.getDocumentElement();
        System.out.println("Root element name: " + parsedRoot.getTagName());
        System.out.println("sensorId attribute: " + parsedRoot.getAttribute("sensorId"));

        NodeList readingNodes = parsedRoot.getElementsByTagName("reading");
        NodeList notesNodes = parsedRoot.getElementsByTagName("notes");
        System.out.println("reading element text: " + readingNodes.item(0).getTextContent());
        System.out.println("notes element text (correctly un-escaped back to a real '&'): " + notesNodes.item(0).getTextContent());
        System.out.println("(getElementsByTagName() + getTextContent()/getAttribute() is how you navigate a DOM tree once");
        System.out.println("parsed - conceptually identical to walking a Jackson JsonNode tree, just XML's node/attribute");
        System.out.println("shape instead of JSON's object/array/value shape.)");

        System.out.println();
        System.out.println("=== Section 4: where this fits, and where it deliberately doesn't ===");
        System.out.println("This project doesn't use XML anywhere - Kafka payloads are JSON (Step 5), the database is Postgres,");
        System.out.println("there's no SOAP endpoint here. But DOM (or SAX/StAX for large documents) is exactly what you'd");
        System.out.println("reach for integrating with a legacy enterprise system that speaks XML/SOAP - genuinely common at");
        System.out.println("banks, insurers, and large enterprises still running systems built before JSON/REST became standard.");
        System.out.println("This is a survival-level skill for this project specifically, not a skill this project needs day-to-day.");
    }
}