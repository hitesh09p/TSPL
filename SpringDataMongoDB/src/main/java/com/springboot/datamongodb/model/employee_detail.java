package com.springboot.datamongodb.model;
import java.util.ArrayList;
import java.util.List;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection="test")
public class employee_detail {
	
	private String _id ;
	private int emp_id;
	private int emp_name;
	
	
	
	
	protected employee_detail() { this._id = new String();}

	public employee_detail (String _id, int emp_id, int emp_name ) {
		super();
		this._id = _id;
		this.emp_id = emp_id;
		this.emp_name = emp_name;
	}
	

	public int getEmp_id() {
		return emp_id;
	}

	

	public int getEmp_name() {
		return emp_name;
	}
	public String getId() {
		return _id;
	}

	/*public void setId String _id) {
		this._id = _id;
	}*/
	/*public void setDashboard(List<db_detail> Dashboard) {
		this.Dashboard = Dashboard;
	}
*/	
	@Override
	public String toString() {
		return String.format(
				"employee_detail [emp_id=%i, emp_name=%i, _id=%s]", _id ,emp_id,emp_name);
	}


	
	

}
