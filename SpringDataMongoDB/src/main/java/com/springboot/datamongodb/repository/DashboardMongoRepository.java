/*package com.springboot.datamongodb.repository;

import java.util.List;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import com.springboot.datamongodb.model.DashboardNew;
@Repository
public interface DashboardMongoRepository extends MongoRepository<DashboardNew, String>{
	//List<Dashboard> findByid();
	
	List<DashboardNew> findById(String id);
	
	@Query(value="{}", fields="{_id:0 ,dashboard:1 }")
	List<Dashboard> findBydash();

	//List<Dashboard> getbrowser(String fromdate , String todate);
	
}*/
