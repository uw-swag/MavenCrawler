package ca.uwaterloo.swag.mavencrawler.pojo;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.bson.Document;
import org.bson.codecs.pojo.annotations.BsonIgnore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOptions;

public class Metadata {

	public static final String METADATA_COLLECTION = "Metadata";

	private String groupId;
	private String artifactId;
	private String repository;
	private String latest;
	private String release;
	private List<String> versions = new ArrayList<String>();
	private Date lastUpdated;
	
	@BsonIgnore
	private List<URL> librariesURLs;
	
	public String getGroupId() {
		return groupId;
	}
	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}
	public String getArtifactId() {
		return artifactId;
	}
	public void setArtifactId(String artifactId) {
		this.artifactId = artifactId;
	}
	public String getRepository() {
		return repository;
	}
	public void setRepository(String repository) {
		this.repository = repository;
	}
	public String getLatest() {
		return latest;
	}
	public void setLatest(String latest) {
		this.latest = latest;
	}
	public String getRelease() {
		return release;
	}
	public void setRelease(String release) {
		this.release = release;
	}
	public List<String> getVersions() {
		return versions;
	}
	public void setVersions(List<String> versions) {
		this.versions = versions;
	}
	public Date getLastUpdated() {
		return lastUpdated;
	}
	public void setLastUpdated(Date lastUpdated) {
		this.lastUpdated = lastUpdated;
	}
	
	public List<URL> getLibrariesURLs() {
		
		if (librariesURLs == null ) {

			librariesURLs = new ArrayList<>();
			
			if (this.getVersions() != null) {
				
				for (String version : this.getVersions()) {

					String baseURL = (this.getRepository() + "/").replaceAll("(?<!(http:|https:))//", "/");
					try {
						URI libraryURI = new URI(baseURL).resolve(
											this.getGroupId().replaceAll("\\.", "/") + "/" + 
											this.getArtifactId() + "/" +
											version + "/" + 
											this.getArtifactId() + "-" + version + ".jar");
						librariesURLs.add(libraryURI.toURL());
					} catch (URISyntaxException | MalformedURLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
				
			}
		}
		
		return librariesURLs;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((artifactId == null) ? 0 : artifactId.hashCode());
		result = prime * result + ((groupId == null) ? 0 : groupId.hashCode());
		result = prime * result + ((lastUpdated == null) ? 0 : lastUpdated.hashCode());
		result = prime * result + ((latest == null) ? 0 : latest.hashCode());
		result = prime * result + ((release == null) ? 0 : release.hashCode());
		result = prime * result + ((repository == null) ? 0 : repository.hashCode());
		result = prime * result + ((versions == null) ? 0 : versions.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Metadata other = (Metadata) obj;
		if (artifactId == null) {
			if (other.artifactId != null)
				return false;
		} else if (!artifactId.equals(other.artifactId))
			return false;
		if (groupId == null) {
			if (other.groupId != null)
				return false;
		} else if (!groupId.equals(other.groupId))
			return false;
		if (lastUpdated == null) {
			if (other.lastUpdated != null)
				return false;
		} else if (!lastUpdated.equals(other.lastUpdated))
			return false;
		if (latest == null) {
			if (other.latest != null)
				return false;
		} else if (!latest.equals(other.latest))
			return false;
		if (release == null) {
			if (other.release != null)
				return false;
		} else if (!release.equals(other.release))
			return false;
		if (repository == null) {
			if (other.repository != null)
				return false;
		} else if (!repository.equals(other.repository))
			return false;
		if (versions == null) {
			if (other.versions != null)
				return false;
		} else if (!versions.equals(other.versions))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "Metadata [groupId=" + groupId + ", artifactId=" + artifactId + ", repository=" + repository
				+ ", latest=" + latest + ", release=" + release + ", versions=" + versions + ", lastUpdated="
				+ lastUpdated + "]";
	}

	public static void checkIndexesInCollection(MongoCollection<Metadata> collection) {
		IndexOptions indexOptions = new IndexOptions().unique(true);
		collection.createIndex(Indexes.ascending("groupId", "artifactId"), indexOptions);
	}
	
	/**
	 * If Metadata already exists for given groupId and artifactId, versions are appended distinctively.
	 * @param metadata
	 * @param mongoDatabase
	 * @param logger
	 */
	public static void upsertInMongo(Metadata metadata, MongoDatabase mongoDatabase, Logger logger) {
		
		MongoCollection<Metadata> collection = mongoDatabase.getCollection(METADATA_COLLECTION, Metadata.class);
		
		Metadata oldMetadata = collection.find(
				and(eq("groupId", metadata.getGroupId()), 
					eq("artifactId", metadata.getArtifactId())))
				.first();
		
		// Check if upserting new metadata
		if (oldMetadata != null && oldMetadata.getLastUpdated() != null && metadata.getLastUpdated() == null) {
			return;
		}
		else if (oldMetadata != null && 
				 oldMetadata.getLastUpdated() != null && 
				 metadata.getLastUpdated() != null && 
				 oldMetadata.getLastUpdated().compareTo(metadata.getLastUpdated()) >= 0) {
			return;
		}
		
		if (oldMetadata != null && oldMetadata.getVersions() != null) {
			List<String> newVersions = new ArrayList<>(metadata.getVersions());
			newVersions.addAll(oldMetadata.getVersions());
			newVersions = newVersions.stream().distinct().collect(Collectors.toList());
			newVersions.sort(String::compareTo);
			metadata.setVersions(newVersions);
		}
		
		collection.updateOne(
				and(eq("groupId", metadata.getGroupId()), 
					eq("artifactId", metadata.getArtifactId())), 
				new Document("$set", metadata), 
				new UpdateOptions().upsert(true));
	}
	
	public static List<Metadata> findAllFromMongo(MongoDatabase mongoDatabase) {
		MongoCollection<Metadata> collection = mongoDatabase.getCollection(METADATA_COLLECTION, Metadata.class);
		return collection.find().into(new ArrayList<Metadata>());
	}

}
