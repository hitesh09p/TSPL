# hbase-compare
Utility to compare 2 HBase tables

----------------------
Introduction
----------------------
HBase Compare utility is used to compare two HBase tables for the data taking into account the versions in HBase. The utility is provisioned with options to perform comparison only for specific column families, column qualifiers, skipping timestamp versions. The comparison is implemented using the MapReduce approach. MapReduce implementation helps in parallelizing the processing of big data from HBase tables utilizing the MapReduce API.

----------------------
Modules
----------------------
The utility is made up of the following components
-	Driver
-	Mapper
-	Reducer
-	Custom Writable

----------------------
Requirement
----------------------
The following runtime requirements are required for running the utility.
-	CDH 5.1 or above
-	HBase 
-	JRE 1.7 or above

----------------------
Execution
----------------------
The HBase compare utility has to be run as hadoop utility. Before running the utility, hadoop and hbase jars has to be added to the classpath as

- export HADOOP_CLASSPATH=\`hadoop classpath\`:\`hbase classpath\`:\<path of hbase-site.xml\>

The utility is run using the following command. (optional arguments are specified in [])

- hadoop jar HbaseCompare-1.0.jar com.pponna.hbase.compare.HbaseCompare -tbl1 \<table1\> -tbl2 \<table2\> -outPath \<hdfsOutputPath\> [-vof \<FieldsDelimitedByComma\>] [-skipCols \<ColumnsDelimitedByComma\>] [-skipFamily \<ColumnFamiliesDelimitedByComma\>] [-numRed \<NumberOfReducers\>] -libjars \<HbaseJarsDelimitedByComma\> [-skipTs]

----------------------
Command Line arguments
----------------------
- tbl1			-	HBase table name
- tbl2			-	HBase table name
- outPath		-	Hdfs output path where the output will be generated
- vof			-	Column qualifiers delimited by comma whose value only will be compared neglecting the timestamp. 
- skipCols		-	Column qualifiers delimited by comma which will be skipped during comparison
- skipFamily		-	Column Families delimited by comma which will be skipped during comparison
- numRed			-	Number of reducers
- libjars		-	HBase jars delimited by comma 
- skipTs			-	Skips the timestamp comparison (only values will be compared) for the entire HBase table

----------------------
Utility Output
----------------------
On running the utility, the last logging statement denotes whether the HBase tables match or not. If HBase tables match, "Tables Match" will be logged. If the comparison results in mismatch, 2 files will be generated at the hdfs output path specified in the argument (outPath). The following files will be generated at the output path
- missing_rowkeys		-	This file will list the rowkeys missing in one of the tables. The info will be logged as tablename with missing rowkey
- mismatch_records	-	This file will list the columns for each rowkey which doesn't match between the tables
