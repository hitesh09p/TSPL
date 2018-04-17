package com.springboot.datamongodb.queries;
import java.util.Arrays;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
//import com.mongodb.client.MongoDatabase;
import com.springboot.datamongodb.dbConnection;
public class emp_query {

	
	 private dbConnection db = new dbConnection();
	    MongoClient mongoClient = db.GetDBConnection();
	  
	    //MongoDatabase database  = mongoClient.getDatabase(")
	    MongoCollection<Document> collection = mongoClient.getDatabase("test").getCollection("employee");
	
	
	    public AggregateIterable<Document>  getEmpCollection() {
	    	
	    	AggregateIterable<Document> emp_detail = collection.aggregate(Arrays.asList(
	               // new Document("$match", new Document("_id", fromdate)),    
	                new Document("$project", new Document("_id", 1)
	                                .append("emp_id", 1)
	                                .append("emp_name", 1))));
	         for (Document dbObject : emp_detail )
	          {
	              System.out.println(dbObject);
	          }
	         
	    return emp_detail;

	    }
	
	
}
