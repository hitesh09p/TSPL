package com.pponna.hbase.compare;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pponna.hbase.mapreduce.HbaseCompareDriver;

/**
 * The HbaseCompare.java.
 *
 * @author Pratheesh
 */
public class HbaseCompare {

    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseCompare.class);


    /**
     * Method to trigger utility
     * @param args
     */
    public static void main(String[] args) {

        HbaseCompare hbaseCompare = new HbaseCompare();
        hbaseCompare.init(args);
    }

    /**
     * Method to initialize utility arguments
     * @param args
     */
    private void init(String[] args) {

        CommandLineParser parser = new PosixParser();
        Options options = new Options();
        options.addOption("tbl1", true, "Name of Hbase table 1");
        options.addOption("tbl2", true, "Name of Hbase table 2");
        options.addOption("vof", true, "Value Only Fields");
        options.addOption("skipCols", true, "Skip Fields");
        options.addOption("skipFamily", true, "Skip Column Family");
        options.addOption("outPath", true, "hdfs output path");
        options.addOption("numRed", true, "Number of Reducers");
        options.addOption("libjars", true, "Number of Reducers");
        options.addOption("skipTS", false, "Skip Column Timestamp");
        String hbaseTable1 = null;
        String hbaseTable2 = null;
        String valueOnlyFields = null;
        String skipFields = null;
        String skipColFamily = null;
        String outPath = null;
        String numRed = null;
        String libjars = null;
        boolean skipTS = false;
        int reducers = 0;
        try {
            CommandLine cli = parser.parse(options, args, false);
            hbaseTable1 = cli.getOptionValue("tbl1");
            hbaseTable2 = cli.getOptionValue("tbl2");
            valueOnlyFields = cli.getOptionValue("vof");
            skipFields = cli.getOptionValue("skipCols");
            skipColFamily = cli.getOptionValue("skipFamily");
            outPath = cli.getOptionValue("outPath");
            numRed = cli.getOptionValue("numRed");
            libjars = cli.getOptionValue("libjars");
            skipTS = cli.hasOption("skipTS");
            reducers = validateInput(hbaseTable1, hbaseTable2, outPath, numRed);
        } catch (ParseException e) {
            LOGGER.error("Parse Exception :: {}", e.getMessage());
            LOGGER.info("Usage : java HbaseCompare -tbl1 <hbase table1> -tbl2 <hbase table2> -outPath <HDFS output path> [-vof <value only fields>] [-skipCols <Fields to be skipped>] [-skipFamily <Column Family to be skipped>] [-numRed <Number of reducers>] [-libjars <jars to be added to job classpath>] [-skipTS]");
            System.exit(-1);
        }

        compare(hbaseTable1, hbaseTable2, valueOnlyFields, skipFields, skipColFamily, outPath, reducers, libjars,
                skipTS);
    }

    /**
     * Method to validate input arguments
     * 
     * @param hbaseTable1
     * @param hbaseTable2
     * @param outPath
     * @param numRed
     * @param reducers
     * @return
     * @throws ParseException
     */
    private int validateInput(String hbaseTable1, String hbaseTable2, String outPath, String numRed)
            throws ParseException {

        int reducers = 10;
        if (StringUtils.isBlank(hbaseTable1) || StringUtils.isBlank(hbaseTable2) || StringUtils.isBlank(outPath)) {
            throw new ParseException("Invalid Option");
        }
        if (!StringUtils.isBlank(numRed)) {
            if (!StringUtils.isNumeric(numRed)) {
                throw new ParseException("Invalid Number of Reducers");
            }
            reducers = Integer.parseInt(numRed);
        }

        if (!hbaseTable1.trim().equals("") && !hbaseTable2.trim().equals("")
                && hbaseTable1.trim().equalsIgnoreCase(hbaseTable2.trim())) {
            LOGGER.info("Tables provided for comparison are same. Check the input table names");
            System.exit(-1);
        }
        return reducers;
    }

    /**
     * Method to invoke utility mapreduce driver
     * @param hbaseTbl1
     * @param hbaseTbl2
     * @param valueOnlyFields
     * @param skipFields
     * @param skipColFamily
     * @param outPath
     * @param reducers
     * @param libjars
     * @param skipTS
     */
    private void compare(String hbaseTbl1, String hbaseTbl2, String valueOnlyFields, String skipFields,
            String skipColFamily, String outPath, int reducers, String libjars, boolean skipTS) {

        HbaseCompareDriver driver = new HbaseCompareDriver();
        try {
            driver.startDriver(hbaseTbl1, hbaseTbl2, valueOnlyFields, skipFields, skipColFamily, outPath, reducers,
                    libjars, skipTS);
        } catch (ClassNotFoundException  e) {
            LOGGER.error(getExceptionMessage(e));
        }
        catch (IOException  e) {
            LOGGER.error(getExceptionMessage(e));
        }
        catch ( InterruptedException e) {
            LOGGER.error(getExceptionMessage(e));
        }
    }

    /**
     * Method to get exception details
     * @param e
     * @return
     */
    public String getExceptionMessage(Throwable e) {

        return (e.getCause() != null) ? e.getCause().getMessage() : e.getMessage();
    }
}
