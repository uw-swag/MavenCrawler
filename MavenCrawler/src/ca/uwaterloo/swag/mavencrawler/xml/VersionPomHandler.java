package ca.uwaterloo.swag.mavencrawler.xml;

import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import ca.uwaterloo.swag.mavencrawler.pojo.VersionPom;

/**
 * XML handler to handle version POM files from the leaf version folder of a library in a Maven repository.
 * 
 * @author cbdeassi
 */
public class VersionPomHandler extends DefaultHandler {
	
	private enum ElementType {
		PROJECT,
		GROUPID,
		ARTIFACTID,
		NAME,
		VERSION,
		DESCRIPTION,
		PROJECTURL,
		SCM,
		CONNECTION,
		URL,
		NONE,
		IGNORE
	}
	
	private Stack<ElementType> elementStack = new Stack<>();
	private VersionPom versionPom = new VersionPom();

	public VersionPom getVersionPom() {
		return versionPom;
	}
	
	@Override
	public void startDocument() throws SAXException {
		super.startDocument();
		elementStack.push(ElementType.NONE);
	}
	
	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		
		super.startElement(uri, localName, qName, attributes);
		
		try {
			ElementType currentElementType = ElementType.valueOf(qName.toUpperCase());
			elementStack.push(currentElementType);
		} catch (Exception e) {
			// If an unknown element is found, just ignore
			elementStack.push(ElementType.IGNORE);
		}
	}
	
	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		super.endElement(uri, localName, qName);
		
		// Release current element
		elementStack.pop();
	}
	
	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		super.characters(ch, start, length);
		String value = new String(ch, start, length);
		
		ElementType currentElement = elementStack.pop();
		
		switch (currentElement) {
		case GROUPID:
			if (elementStack.peek() == ElementType.PROJECT) {
				versionPom.setGroupId(value.trim());
			}
			break;
		case ARTIFACTID:
			if (elementStack.peek() == ElementType.PROJECT) {
				versionPom.setArtifactId(value);
			}
			break;
		case NAME:
			if (elementStack.peek() == ElementType.PROJECT) {
				versionPom.setName(value);
			}
			break;
		case VERSION:
			if (elementStack.peek() == ElementType.PROJECT) {
				versionPom.setVersion(value);
			}
			break;
		case DESCRIPTION:
			if (elementStack.peek() == ElementType.PROJECT) {
				versionPom.setDescription(value);
			}
			break;
		case URL:
			
			switch (elementStack.peek()) {
			case PROJECT:
				versionPom.setProjectUrl(value.replaceAll("\\s+", ""));
				break;
			case SCM:
				versionPom.setScmUrl(value.replaceAll("\\s+", ""));
				break;
			default:
				break;
			}
			
			break;
		case CONNECTION:
			if (elementStack.peek() == ElementType.SCM) {
				versionPom.setScmConnection(value.replaceAll("\\s+", ""));
			}
			break;
		default:
			break;
		}
		
		elementStack.push(currentElement);
	}

	
	@Override
	public void endDocument() throws SAXException {
		super.endDocument();
		elementStack = new Stack<>();
	}

}
