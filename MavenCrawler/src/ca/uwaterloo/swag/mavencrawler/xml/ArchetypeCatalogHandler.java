package ca.uwaterloo.swag.mavencrawler.xml;

import java.util.ArrayList;
import java.util.List;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import ca.uwaterloo.swag.mavencrawler.pojo.Archetype;

/**
 * XML handler to handle archetype-catalog.xml file from the root of a Maven repository.
 * 
 * @author cbdeassi
 */
public class ArchetypeCatalogHandler extends DefaultHandler {
	
	private enum ElementType {
		ARCHETYPE,
		GROUPID,
		ARTIFACTID,
		VERSION,
		DESCRIPTION,
		NONE
	}

	private ElementType currentElementType = ElementType.NONE;
	private Archetype currentArchetype = null;
	private List<Archetype> archetypes = new ArrayList<Archetype>();
	
	public List<Archetype> getArchetypes() {
		return archetypes;
	}
	
	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		super.startElement(uri, localName, qName, attributes);

		try {
			currentElementType = ElementType.valueOf(qName.toUpperCase());
		} catch (Exception e) {
			// If an unknown element is found
			currentElementType = ElementType.NONE;
		}
		
		if (currentElementType == ElementType.ARCHETYPE) {
			currentArchetype = new Archetype();
			this.archetypes.add(currentArchetype);
		}
	}
	
	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		super.endElement(uri, localName, qName);
		
		// Release current element
		currentElementType = ElementType.NONE;
	}
	
	@Override
	// Reading data from inside an element
	public void characters(char[] ch, int start, int length) throws SAXException {
		super.characters(ch, start, length);
		
		String value = new String(ch, start, length);
		
		switch (currentElementType) {
		case GROUPID:
			currentArchetype.setGroupId(value);
			break;
		case ARTIFACTID:
			currentArchetype.setArtifactId(value);
			break;
		case VERSION:
			currentArchetype.setVersion(value);
			break;
		case DESCRIPTION:
			currentArchetype.setDescription(value);
			break;
		default:
			break;
		}
	}
	
	@Override
	public void endDocument() throws SAXException {
		super.endDocument();
	}

}
