package de.metanome.algorithms.sindd.database;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * 
 * @author Nuhad.Shaabani
 * 
 */
public class DBConn {

	private static final Logger LOGGER = Logger.getLogger(DBConn.class);

	private Connection connection;

	public DBConn(Connection connection){
		this.connection = connection;
		LOGGER.info("the database connection is created.");
	}

	public Statement getStatement() {
		Statement statement = null;
		try {
			statement = connection.createStatement();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return statement;
	}

	public PreparedStatement getPreparedStatement(String sqlStatement) {
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(sqlStatement);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return statement;
	}

	public DatabaseMetaData getDatabaseMetaData() {
		DatabaseMetaData metaData = null;
		try {
			metaData = connection.getMetaData();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return metaData;
	}

	public void close() {
		try {
			connection.close();
			LOGGER.info("the database connection is closed.");
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
