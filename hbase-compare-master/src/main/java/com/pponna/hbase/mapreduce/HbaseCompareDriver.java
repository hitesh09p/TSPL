package com.pponna.hbase.mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pponna.util.UtilityConstants;

/**
 * The HbaseCompareDriver.java.
 *
 * @author Pratheesh
 */
public class HbaseCompareDriver {

    private static final Logger DRLOGGER = LoggerFactory.getLogger(HbaseCompareDriver.class);

    private static final String FILE_SEPARATOR = System.getProperty("file.separator");

    /**
     * Method to initialize driver for compare MR
     * @param tblName1
     * @param tblName2
     * @param valueOnlyFields
     * @param skipFields
     * @param skipColFamily
     * @param outPath
     * @param reducers
     * @param libjars
     * @param skipTS
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void startDriver(String tblName1, String tblName2, String valueOnlyFields, String skipFields,
            String skipColFamily, String outPath, int reducers, String libjars, boolean skipTS) throws IOException,
            ClassNotFoundException, InterruptedException {

        List<Scan> scans = createScan(tblName1, tblName2, skipTS);

        Configuration configuration = new Configuration();
        configuration.set("table1", tblName1);
        configuration.set("table2", tblName2);
        if (valueOnlyFields != null) {
            configuration.set("ValueOnlyFields", valueOnlyFields);
        }
        if (skipFields != null) {
            configuration.set("SkipFields", skipFields);
        }
        if (skipColFamily != null) {
            configuration.set("SkipColFamily", skipColFamily);
        }
        configuration.set("skipTS", Boolean.toString(skipTS));
        if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
            configuration.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
        }
        UserGroupInformation.setConfiguration(configuration);
        configuration = HBaseConfiguration.create(configuration);
        Job job = Job.getInstance(configuration);
        job.setJobName("Compare:" + tblName1 + "--" + tblName2);
        job.setJarByClass(HbaseCompareDriver.class);
        String outputPath = new StringBuffer(new Path(outPath).toString()).append(FILE_SEPARATOR).append(tblName1)
                .append("_").append(tblName2).toString();
        FileSystem fs = FileSystem.newInstance(job.getConfiguration());
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }

        String namenode = job.getConfiguration().get(UtilityConstants.HADOOP_NAMENODE);
        job.setReducerClass(HbaseCompareReducer.class);
        if (reducers != 0) {
            job.setNumReduceTasks(reducers);
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        TableMapReduceUtil.setScannerCaching(job, 100);
        StringBuilder commonPath = new StringBuilder();
        commonPath.append(namenode);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValueListWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        if (!org.apache.commons.lang.StringUtils.isBlank(libjars)) {
            job.getConfiguration().set("tmpjars", validateFiles(libjars, job.getConfiguration()));
        }
        TableMapReduceUtil.initCredentials(job);
        TableMapReduceUtil.initTableMapperJob(scans, HbaseCompareMapper.class, ImmutableBytesWritable.class,
                KeyValueListWritable.class, job, false);

        boolean jobStatus = job.waitForCompletion(true);
        if (job.getStatus().getState() == JobStatus.State.FAILED
                || job.getStatus().getState() == JobStatus.State.KILLED) {
            DRLOGGER.error("Job Failed !!! Please check Job Tracker Logs for cause");
            System.exit(-1);
        }
        validateOutput(tblName1, tblName2, job, outputPath, fs, jobStatus);
    }

    /**
     * Method to create hbase scan objects
     * 
     * @param tblName1
     * @param tblName2
     * @param skipTS
     * @return
     */
    private List<Scan> createScan(String tblName1, String tblName2, boolean skipTS) {

        List<Scan> scans = new ArrayList<Scan>();
        Scan scanTbl1 = new Scan();
        scanTbl1.setAttribute("scan.attributes.table.name", Bytes.toBytes(tblName1));
        if (skipTS) {
            scanTbl1 = scanTbl1.setMaxVersions(1);
        } else {
            scanTbl1 = scanTbl1.setMaxVersions();
        }
        scans.add(scanTbl1);
        Scan scanTbl2 = new Scan();
        scanTbl2.setAttribute("scan.attributes.table.name", Bytes.toBytes(tblName2));
        if (skipTS) {
            scanTbl2 = scanTbl2.setMaxVersions(1);
        } else {
            scanTbl2 = scanTbl2.setMaxVersions();
        }
        scans.add(scanTbl2);
        return scans;
    }

    /**
     * Method to validate MR output
     * 
     * @param tblName1
     * @param tblName2
     * @param job
     * @param outputPath
     * @param fs
     * @param jobStatus
     * @throws IOException
     * @throws FileNotFoundException
     */
    private void validateOutput(String tblName1, String tblName2, Job job, String outputPath, FileSystem fs,
            boolean jobStatus) throws IOException {

        boolean status = jobStatus;
        boolean fail = false;
        if (status) {
            String sourcePath = new StringBuffer(outputPath).toString();
            FileSystem sourceFileSystem = FileSystem.newInstance(job.getConfiguration());
            FileStatus[] files = sourceFileSystem.listStatus(new Path(sourcePath));

            String outFilePath = sourcePath;
            if (sourcePath.charAt(sourcePath.length() - 1) != FILE_SEPARATOR.toCharArray()[0]) {
                outFilePath += FILE_SEPARATOR;
            }
            Path missingFilePath = new Path(outFilePath + "missing");
            if (!sourceFileSystem.exists(missingFilePath)) {
                sourceFileSystem.mkdirs(missingFilePath);
            }
            Path failedFilePath = new Path(outFilePath + "failed");
            if (!sourceFileSystem.exists(failedFilePath)) {
                sourceFileSystem.mkdirs(failedFilePath);
            }

            int length = files.length;
            for (int i = 0; i < length; i++) {
                if (files[i].getPath().getName().contains("missing_rowkeys-r")
                        || files[i].getPath().getName().contains("failure-r")) {
                    if (files[i].getLen() != 0) {
                        status = false;
                        fail = true;
                        if (files[i].getPath().getName().contains("missing_rowkeys-r")) {

                            FileUtil.copy(sourceFileSystem, files[i].getPath(), sourceFileSystem, missingFilePath,
                                    true, job.getConfiguration());

                        }
                        if (files[i].getPath().getName().contains("failure-r")) {

                            FileUtil.copy(sourceFileSystem, files[i].getPath(), sourceFileSystem, failedFilePath, true,
                                    job.getConfiguration());

                        }
                    }
                } else {
                    fs.delete(files[i].getPath(), true);
                }
            }

            if (sourceFileSystem.listStatus(missingFilePath).length > 0) {
                String mergedMissingFile = new StringBuffer(outputPath).append(FILE_SEPARATOR)
                        .append("missing_rowkeys").toString();
                FileUtil.copyMerge(sourceFileSystem, missingFilePath, sourceFileSystem, new Path(mergedMissingFile),
                        true, job.getConfiguration(), null);
            } else {
                fs.delete(missingFilePath, true);
            }
            if (sourceFileSystem.listStatus(failedFilePath).length > 0) {
                String mergedFailedFile = new StringBuffer(outputPath).append(FILE_SEPARATOR)
                        .append("mismatch_records").toString();
                FileUtil.copyMerge(sourceFileSystem, failedFilePath, sourceFileSystem, new Path(mergedFailedFile),
                        true, job.getConfiguration(), null);
            } else {
                fs.delete(failedFilePath, true);
            }

        }

        if (status || !fail) {
            DRLOGGER.info("Tables {} , {} match", tblName1, tblName2);
        } else {
            DRLOGGER.info("Tables do not match. Please check error files under {} for details", outputPath);
        }
    }

    /**
     * Method to validate tmp jars
     * @param files
     * @param conf
     * @return
     * @throws IOException
     */
    private String validateFiles(String files, Configuration conf) throws IOException {

        if (files == null) {
            return null;
        }
        String[] fileArr = files.split(",");
        String[] finalArr = new String[fileArr.length];
        for (int i = 0; i < fileArr.length; i++) {
            String tmp = fileArr[i];
            String finalPath;
            URI pathURI;
            try {
                pathURI = new URI(tmp);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
            Path path = new Path(pathURI);
            FileSystem localFs = FileSystem.getLocal(conf);
            if (pathURI.getScheme() == null) {
                if (!localFs.exists(path)) {
                    throw new FileNotFoundException("File " + tmp + " does not exist.");
                }
                finalPath = path.makeQualified(localFs).toString();
            } else {
                FileSystem fs = path.getFileSystem(conf);
                if (!fs.exists(path)) {
                    throw new FileNotFoundException("File " + tmp + " does not exist.");
                }
                finalPath = path.makeQualified(fs).toString();
            }
            finalArr[i] = finalPath;
        }
        return StringUtils.arrayToString(finalArr);
    }
}
