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

public class Archetype {
	
	public static final String ARCHETYPE_COLLECTION = "Archetypes";
	
	private String groupId;
	private String artifactId;
	private String version;
	private String repository;
	private String description;
	
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
	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	public String getRepository() {
		return repository;
	}
	public void setRepository(String repository) {
		this.repository = repository;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((artifactId == null) ? 0 : artifactId.hashCode());
		result = prime * result + ((description == null) ? 0 : description.hashCode());
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
		Archetype other = (Archetype) obj;
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
		return "Archetype [groupId=" + groupId + ", artifactId=" + artifactId + ", version=" + version + ", repository="
				+ repository + ", description=" + description + "]";
	}
	public static void checkIndexesInCollection(MongoCollection<Archetype> collection) {
		IndexOptions indexOptions = new IndexOptions().unique(true);
		collection.createIndex(Indexes.ascending("groupId", "artifactId", "version"), indexOptions);
	}
	
	public static void upsertInMongo(List<Archetype> archetypesList, MongoDatabase mongoDatabase, Logger logger) {
		
		List<UpdateOneModel<Archetype>> upsertRequests = new ArrayList<UpdateOneModel<Archetype>>(archetypesList.size());
		UpdateOptions updateOptions = new UpdateOptions().upsert(true);
		
		for (Archetype archetype : archetypesList) {
			upsertRequests.add(new UpdateOneModel<Archetype>(
					and(eq("groupId", archetype.getGroupId()), 
						eq("artifactId", archetype.getArtifactId()),
						eq("version", archetype.getVersion())), 
					new Document("$set", archetype), 
					updateOptions));
		}

		LoggerHelper.log(logger, Level.INFO, "Saving " + upsertRequests.size() + " upserts to database...");
		
		MongoCollection<Archetype> collection = mongoDatabase.getCollection(ARCHETYPE_COLLECTION, Archetype.class);
		BulkWriteResult result = collection.bulkWrite(upsertRequests);

		LoggerHelper.log(logger, Level.INFO, "Matched: " + result.getMatchedCount() + 
				". Inserted: " + result.getInsertedCount() +
				". Modified:" + result.getModifiedCount() + ".");
		
	}

}
