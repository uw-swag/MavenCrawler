package ca.uwaterloo.swag.mavencrawler.pojo;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;

import java.util.Date;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Repository {
	
	private static final String REPOSITORY_COLLECTION = "Repositories";
	
	private String url;
	private Date lastChecked;
	private Date lastUpdated;

	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public Date getLastChecked() {
		return lastChecked;
	}
	public void setLastChecked(Date lastChecked) {
		this.lastChecked = lastChecked;
	}
	public Date getLastUpdated() {
		return lastUpdated;
	}
	public void setLastUpdated(Date lastUpdated) {
		this.lastUpdated = lastUpdated;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((lastChecked == null) ? 0 : lastChecked.hashCode());
		result = prime * result + ((lastUpdated == null) ? 0 : lastUpdated.hashCode());
		result = prime * result + ((url == null) ? 0 : url.hashCode());
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
		Repository other = (Repository) obj;
		if (lastChecked == null) {
			if (other.lastChecked != null)
				return false;
		} else if (!lastChecked.equals(other.lastChecked))
			return false;
		if (lastUpdated == null) {
			if (other.lastUpdated != null)
				return false;
		} else if (!lastUpdated.equals(other.lastUpdated))
			return false;
		if (url == null) {
			if (other.url != null)
				return false;
		} else if (!url.equals(other.url))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "Repository [url=" + url + ", lastChecked=" + lastChecked + ", lastUpdated=" + lastUpdated + "]";
	}
	
	public static Date getLastCheckedDateForURLInMongo(String url, MongoDatabase db) {
		
		MongoCollection<Repository> collection = db.getCollection(REPOSITORY_COLLECTION, Repository.class);
		Repository repo = getRepoByURL(url, collection);
		
		return repo.getLastChecked();
	}
	
	public static void setLastCheckedDateForURLInMongo(String url, MongoDatabase db, Date date) {
		
		MongoCollection<Repository> collection = db.getCollection(REPOSITORY_COLLECTION, Repository.class);
		Repository repo = getRepoByURL(url, collection);
		repo.setLastChecked(date);
		collection.updateOne(eq("url", url), set("lastChecked", date));
	}
	
	// Helpers
	
	private static Repository getRepoByURL(String url, MongoCollection<Repository> collection) {

		Repository repo = collection.find(eq("url", url)).first();
		
		if (repo == null) {
			repo = new Repository();
			repo.url = url;
			collection.insertOne(repo);
		}
		
		return repo;
	}
}
