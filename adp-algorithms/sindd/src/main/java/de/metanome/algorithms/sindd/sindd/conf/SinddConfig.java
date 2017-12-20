package de.metanome.algorithms.sindd.sindd.conf;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;

/**
 * 
 * @author Nuhad.Shaabani
 *
 */
public class SinddConfig {

	private final static String SHIFT1 = "......";
	private final static String SHIFT2 = "............";

	private final static String DEFAULT_CONF_FILE = "conf/sinddConfig.properties";
	private final static String DEFAULT_LOG4J_PROPS_FILE = "conf/log4j.properties";

	private static String confPropsFileName;
	private static String logPropsFileName;
	private static File confFile;
	private static File log4jPropsFile;
	private static Properties configProps;
	private static Properties log4jProps;

	static {
		confPropsFileName = System.getProperty("sinddConfig");
		if (confPropsFileName == null || confPropsFileName.isEmpty()) {
			confPropsFileName = DEFAULT_CONF_FILE;
		}
		confFile = new File(confPropsFileName);
		if (!confFile.exists()) {
			throw new RuntimeException("S-indd configuration file: " + confFile.getAbsolutePath() + " does not exist!");
		}

		logPropsFileName = System.getProperty("log4jConfig");
		if (logPropsFileName == null || logPropsFileName.isEmpty()) {
			logPropsFileName = DEFAULT_LOG4J_PROPS_FILE;
		}
		log4jPropsFile = new File(logPropsFileName);
		if (!log4jPropsFile.exists()) {
			throw new RuntimeException(
					"log4j properties file: " + log4jPropsFile.getAbsolutePath() + " does not exist");
		}

		loadConfiguration();
		loadLogProperties();
		PropertyConfigurator.configure(log4jProps);
	}

	public static void loadConfiguration() {
		configProps = new Properties();
		try {
			configProps.load(new FileInputStream(confFile));
		} catch (Exception e) {
			throw new RuntimeException(
					"a problem occurs during loading configuration file: " + confFile.getAbsolutePath() + "\n", e);
		}
	}

	public static Properties loadLogProperties() {
		log4jProps = new Properties();
		try {
			log4jProps.load(new FileInputStream(log4jPropsFile));
		} catch (Exception e) {
			throw new RuntimeException(
					"a problem occurs during loading log properties file: " + log4jPropsFile.getAbsolutePath() + "\n",
					e);
		}
		return log4jProps;
	}

	public static Properties getLogProperties() {
		return log4jProps;
	}

	public static String getDBName() {
		String key = "dbName";
		return configProps.getProperty(key);
	}

	public static String getJDBCURL() {
		String propName = "jdbc.url";
		String jdbcUrl = configProps.getProperty(propName);
		return jdbcUrl;
	}

	public static String getJDBCDriver() {
		String propName = "jdbc.driver";
		String jdbcDriver = configProps.getProperty(propName);
		return jdbcDriver;
	}

	public static String getDBUser() {
		String key = "user";
		return configProps.getProperty(key);
	}

	public static String getPassword() {
		String key = "password";
		return configProps.getProperty(key);
	}

	public static String getSchemaNames() {
		String key = "schemas";
		return configProps.getProperty(key);
	}

	public static String getExcludedDataTypes() {
		String key = "excludedTypeNames";
		return configProps.getProperty(key);
	}

	public static File getWorkingDirectory() {
		String key = "workingDir";
		return new File(configProps.getProperty(key));
	}

	public static int getOpenFileNr() {
		String key = "openFileNr";
		int openFileNr = Integer.parseInt(configProps.getProperty(key));
		return openFileNr;
	}

	public static int getPartitionNr() {
		String key = "partitionNr";
		int partitionNr = Integer.parseInt(configProps.getProperty(key));
		return partitionNr;
	}

	public static String getShift1() {
		return SHIFT1;
	}

	public static String getShift2() {
		return SHIFT2;
	}
}
