package com.springboot.datamongodb;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.springboot.datamongodb.model.employee_detail;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.Block;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoServerException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
//import com.springboot.datamongodb.model.DashboardNew;
//import com.springboot.datamongodb.model.id_detail;
//import com.springboot.datamongodb.model.distinctDashboard;
//import com.springboot.datamongodb.repository.DashboardMongoRepository;

import org.json.JSONException;
import org.json.JSONObject;
//import org.json.simple.JSONObject;
@SpringBootApplication
public class SpringDataMongoDbApplication {
	
	/*@Autowired
	DashboardMongoRepository DashboardRepo;*/
	
	
    //MongoServerException
	
	public static void main(String[] args) {
		SpringApplication.run(SpringDataMongoDbApplication.class, args);
	}
		
}




	
	


