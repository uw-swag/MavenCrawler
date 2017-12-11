package ca.uwaterloo.swag.mavencrawler.pojo;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import java.util.Date;
import java.util.logging.Logger;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOptions;

public class Downloaded {

	public static final String DOWNLOADED_COLLECTION = "DownloadedLibraries";

	private String groupId;
	private String artifactId;
	private String repository;
	private String version;
	private Date downloadDate;
	private String downloadPath;
	
	public Downloaded(String groupId, String artifactId, String repository, String version, Date downloadDate,
			String downloadPath) {
		super();
		this.groupId = groupId;
		this.artifactId = artifactId;
		this.repository = repository;
		this.version = version;
		this.downloadDate = downloadDate;
		this.downloadPath = downloadPath;
	}
	
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
	public Date getDownloadDate() {
		return downloadDate;
	}
	public void setDownloadDate(Date downloadDate) {
		this.downloadDate = downloadDate;
	}
	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	public String getDownloadPath() {
		return downloadPath;
	}

	public void setDownloadPath(String downloadPath) {
		this.downloadPath = downloadPath;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((artifactId == null) ? 0 : artifactId.hashCode());
		result = prime * result + ((downloadDate == null) ? 0 : downloadDate.hashCode());
		result = prime * result + ((downloadPath == null) ? 0 : downloadPath.hashCode());
		result = prime * result + ((groupId == null) ? 0 : groupId.hashCode());
		result = prime * result + ((repository == null) ? 0 : repository.hashCode());
		result = prime * result + ((version == null) ? 0 : version.hashCode());
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
		Downloaded other = (Downloaded) obj;
		if (artifactId == null) {
			if (other.artifactId != null)
				return false;
		} else if (!artifactId.equals(other.artifactId))
			return false;
		if (downloadDate == null) {
			if (other.downloadDate != null)
				return false;
		} else if (!downloadDate.equals(other.downloadDate))
			return false;
		if (downloadPath == null) {
			if (other.downloadPath != null)
				return false;
		} else if (!downloadPath.equals(other.downloadPath))
			return false;
		if (groupId == null) {
			if (other.groupId != null)
				return false;
		} else if (!groupId.equals(other.groupId))
			return false;
		if (repository == null) {
			if (other.repository != null)
				return false;
		} else if (!repository.equals(other.repository))
			return false;
		if (version == null) {
			if (other.version != null)
				return false;
		} else if (!version.equals(other.version))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "Downloaded [groupId=" + groupId + ", artifactId=" + artifactId + ", repository=" + repository
				+ ", version=" + version + ", downloadDate=" + downloadDate + ", downloadPath=" + downloadPath + "]";
	}

	public static void checkIndexesInCollection(MongoCollection<Metadata> collection) {
		IndexOptions indexOptions = new IndexOptions().unique(true);
		collection.createIndex(Indexes.ascending("groupId", "artifactId", "repository", "version"), indexOptions);
	}

	public static void upsertInMongo(Downloaded downloaded, MongoDatabase mongoDatabase, Logger logger) {

		MongoCollection<Downloaded> collection = mongoDatabase.getCollection(DOWNLOADED_COLLECTION, Downloaded.class);

		collection.updateOne(
				and(eq("groupId", downloaded.getGroupId()), 
					eq("artifactId", downloaded.getArtifactId()),
					eq("repository", downloaded.getRepository()),
					eq("version", downloaded.getVersion())), 
				new Document("$set", downloaded), 
				new UpdateOptions().upsert(true));
	}

}
