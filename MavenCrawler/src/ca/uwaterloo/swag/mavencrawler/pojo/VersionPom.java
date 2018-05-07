package ca.uwaterloo.swag.mavencrawler.pojo;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.Document;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;

import ca.uwaterloo.swag.mavencrawler.helpers.LoggerHelper;

public class VersionPom {
	
	public static final String VERSIONPOM_COLLECTION = "VersionPoms";

	private String groupId;
	private String artifactId;
	private String name;
	private String version;
	private String description;
	private String projectUrl;
	private String repository;
	private String scmConnection;
	private String scmUrl;
	
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
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getProjectUrl() {
		return projectUrl;
	}
	public void setProjectUrl(String projectUrl) {
		this.projectUrl = projectUrl;
	}
	public String getRepository() {
		return repository;
	}
	public void setRepository(String repository) {
		this.repository = repository;
	}
	public String getScmConnection() {
		return scmConnection;
	}
	public void setScmConnection(String scmConnection) {
		this.scmConnection = scmConnection;
	}
	public String getScmUrl() {
		return scmUrl;
	}
	public void setScmUrl(String scmUrl) {
		this.scmUrl = scmUrl;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((artifactId == null) ? 0 : artifactId.hashCode());
		result = prime * result + ((description == null) ? 0 : description.hashCode());
		result = prime * result + ((groupId == null) ? 0 : groupId.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((projectUrl == null) ? 0 : projectUrl.hashCode());
		result = prime * result + ((repository == null) ? 0 : repository.hashCode());
		result = prime * result + ((scmConnection == null) ? 0 : scmConnection.hashCode());
		result = prime * result + ((scmUrl == null) ? 0 : scmUrl.hashCode());
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
		VersionPom other = (VersionPom) obj;
		if (artifactId == null) {
			if (other.artifactId != null)
				return false;
		} else if (!artifactId.equals(other.artifactId))
			return false;
		if (description == null) {
			if (other.description != null)
				return false;
		} else if (!description.equals(other.description))
			return false;
		if (groupId == null) {
			if (other.groupId != null)
				return false;
		} else if (!groupId.equals(other.groupId))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (projectUrl == null) {
			if (other.projectUrl != null)
				return false;
		} else if (!projectUrl.equals(other.projectUrl))
			return false;
		if (repository == null) {
			if (other.repository != null)
				return false;
		} else if (!repository.equals(other.repository))
			return false;
		if (scmConnection == null) {
			if (other.scmConnection != null)
				return false;
		} else if (!scmConnection.equals(other.scmConnection))
			return false;
		if (scmUrl == null) {
			if (other.scmUrl != null)
				return false;
		} else if (!scmUrl.equals(other.scmUrl))
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
		return "VersionPom [groupId=" + groupId + ", artifactId=" + artifactId + ", name=" + name + ", version="
				+ version + ", description=" + description + ", projectUrl=" + projectUrl + ", repository=" + repository
				+ ", scmConnection=" + scmConnection + ", scmUrl=" + scmUrl + "]";
	}
	
	public static void checkIndexesInCollection(MongoCollection<VersionPom> collection) {
		IndexOptions indexOptions = new IndexOptions().unique(true);
		collection.createIndex(Indexes.ascending("groupId", "artifactId", "version", "repository"), indexOptions);
	}
	
	public static void upsertInMongo(List<VersionPom> versionPomsList, MongoDatabase mongoDatabase, Logger logger) {
		
		List<UpdateOneModel<VersionPom>> upsertRequests = new ArrayList<UpdateOneModel<VersionPom>>(versionPomsList.size());
		UpdateOptions updateOptions = new UpdateOptions().upsert(true);
		
		for (VersionPom versionPom : versionPomsList) {
			upsertRequests.add(new UpdateOneModel<VersionPom>(
					and(eq("groupId", versionPom.getGroupId()), 
						eq("artifactId", versionPom.getArtifactId()),
						eq("version", versionPom.getVersion())), 
					new Document("$set", versionPom), 
					updateOptions));
		}

		LoggerHelper.log(logger, Level.INFO, "Saving " + upsertRequests.size() + " upserts to database...");
		
		MongoCollection<VersionPom> collection = mongoDatabase.getCollection(VERSIONPOM_COLLECTION, VersionPom.class);
		BulkWriteResult result = collection.bulkWrite(upsertRequests);

		LoggerHelper.log(logger, Level.INFO, "Matched: " + result.getMatchedCount() + 
				". Inserted: " + result.getInsertedCount() +
				". Modified:" + result.getModifiedCount() + ".");
	}
	
	public static List<VersionPom> findAllFromMongo(MongoDatabase mongoDatabase) {
		MongoCollection<VersionPom> collection = mongoDatabase.getCollection(VERSIONPOM_COLLECTION, VersionPom.class);
		return collection.find().into(new ArrayList<VersionPom>());
	}

}
