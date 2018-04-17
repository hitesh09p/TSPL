package com.springboot.datamongodb.controller;
import java.net.URI;
import java.util.List;

import org.bson.Document;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
//import com.springboot.datamongodb.model.DashboardNew;
//import com.springboot.datamongodb.model.distinctDashboard;
//import com.springboot.datamongodb.repository.DashboardMongoRepository;
import com.mongodb.client.AggregateIterable;
/*import com.in28minutes.springboot.model.Course;
import com.in28minutes.springboot.model.Dashboard;
import com.in28minutes.springboot.service.DashboardService;
import com.in28minutes.springboot.service.StudentService;;*/
import com.springboot.datamongodb.queries.*;

@RestController
//@RequestMapping("/employee")*/


public class emp_controller {

	
	emp_query objdashboard = new emp_query();
	
	@RequestMapping(value={"/test"}, method = RequestMethod.GET)
	public AggregateIterable<Document>  getEmp() {
	AggregateIterable<Document> objemp = objdashboard.getEmpCollection();
	return objemp;
	}
	
	
	
	
	
	
	
}
