package ca.uwaterloo.swag.mavencrawler.xml;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import ca.uwaterloo.swag.mavencrawler.pojo.Metadata;

/**
 * XML handler to handle maven-metadata.xml file from the root of a library in a Maven repository.
 * 
 * @author cbdeassi
 */
public class MavenMetadataHandler extends DefaultHandler {

	private enum ElementType {
		GROUPID,
		ARTIFACTID,
		LATEST,
		RELEASE,
		VERSION,
		LASTUPDATED,
		NONE
	}
	
	private DateFormat mavenFormat = new SimpleDateFormat("yyyyMMddHHmmss");
	private ElementType currentElementType = ElementType.NONE;
	private Metadata metadata = new Metadata();

	public Metadata getMetadata() {
		return metadata;
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
	}
	
	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		super.endElement(uri, localName, qName);
		
		// Release current element
		currentElementType = ElementType.NONE;
	}
	
	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		super.characters(ch, start, length);
		String value = new String(ch, start, length);
		
		switch (currentElementType) {
		case GROUPID:
			metadata.setGroupId(value);
			break;
		case ARTIFACTID:
			metadata.setArtifactId(value);
			break;
		case LATEST:
			metadata.setLatest(value);
			break;
		case RELEASE:
			metadata.setRelease(value);
			break;
		case VERSION:
			metadata.getVersions().add(value);
			break;
		case LASTUPDATED:
			try {
				metadata.setLastUpdated(mavenFormat.parse(value));
			} catch (ParseException e) {
				// No problem if last updated date is not available, just make sure no garbage is collected.
				metadata.setLastUpdated(null);
			}
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
