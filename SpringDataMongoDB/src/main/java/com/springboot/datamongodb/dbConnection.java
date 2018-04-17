package com.springboot.datamongodb;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoCredential;

import java.util.*;
import java.io.*;

public class dbConnection {
	public MongoClient GetDBConnection() {
		String connstring="";
		
		//MongoClient mongoClient = new MongoClient(new ServerAddress("localhost",27016));
		
		String user; // the user name
		String db; // the name of the database in which the user is defined
		char[] password; // the password as a character array
		// ...
		password = new char[]{'t','e','s','t'};
		
		user = "test";
		db="test";
		//password=['A','n','@','l','y','t','1','c','$'];
		
		MongoCredential credential = MongoCredential.createCredential(user, db, password);
		MongoClient mongoClient = new MongoClient(new ServerAddress("localhost", 27017),
		                                         Arrays.asList(credential));
		
		
//		Properties p=null;
//		try {
//			p = GetProperties();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	    //MongoDatabase database = mongoClient.getDatabase(p.getProperty("lrndb"));
		//MongoDatabase database = mongoClient.getDatabase("test");
	    //MongoCollection<Document> collection = database.getCollection("Dashboard");
	    
		return mongoClient;
	}
	
	private Properties GetProperties() throws IOException{
		Properties p = new Properties();
		try {
			Reader reader=new FileReader("SpringDataMongoDB/src/main/resources/application.properties");
			p.load(reader);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		//p.getProperty("lrndb");
		return p;
	}


}


