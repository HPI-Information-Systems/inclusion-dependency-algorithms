package de.metanome.algorithms.binder;

import java.io.File;
import java.util.ArrayList;
import static java.util.Arrays.asList;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.*;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementBoolean;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementInteger;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementString;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithms.binder.utils.CollectionUtils;
import de.metanome.algorithms.binder.utils.FileUtils;
import de.metanome.algorithms.binder.dao.DB2DataAccessObject;
import de.metanome.algorithms.binder.dao.MySQLDataAccessObject;
import de.metanome.algorithms.binder.dao.PostgreSQLDataAccessObject;

public class BinderDatabaseAlgorithm extends Binder implements InclusionDependencyAlgorithm, TableInputParameterAlgorithm, IntegerParameterAlgorithm, StringParameterAlgorithm, BooleanParameterAlgorithm {

	public enum Database {
		MYSQL, DB2, POSTGRESQL
	}

	public enum Identifier {
		INPUT_DATABASE, INPUT_ROW_LIMIT, DATABASE_NAME, DATABASE_TYPE, INPUT_TABLES, TEMP_FOLDER_PATH, CLEAN_TEMP, DETECT_NARY, MAX_NARY_LEVEL, FILTER_KEY_FOREIGNKEYS, NUM_BUCKETS_PER_COLUMN, MEMORY_CHECK_FREQUENCY, MAX_MEMORY_USAGE_PERCENTAGE
	}
	
	@Override
	public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
		ArrayList<ConfigurationRequirement<?>> configs = new ArrayList<>(8);
//		configs.add(new ConfigurationRequirementDatabaseConnection(BinderDatabaseAlgorithm.Identifier.INPUT_DATABASE.name()));

//		ConfigurationRequirementString databaseType = new ConfigurationRequirementString(BinderDatabaseAlgorithm.Identifier.DATABASE_TYPE.name());
//		String[] defaultDatabaseType = new String[1];
//		defaultDatabaseType[0] = "MYSQL";
//		databaseType.setDefaultValues(defaultDatabaseType);
//		databaseType.setRequired(true);
//		configs.add(databaseType); // TODO: take this from the input source
		
//		ConfigurationRequirementString tableNames = new ConfigurationRequirementString(BinderDatabaseAlgorithm.Identifier.INPUT_TABLES.name(), ConfigurationRequirement.ARBITRARY_NUMBER_OF_VALUES);
//		tableNames.setRequired(true);
//		configs.add(tableNames);
		
		ConfigurationRequirementString tempFolder = new ConfigurationRequirementString(BinderDatabaseAlgorithm.Identifier.TEMP_FOLDER_PATH.name());
		String[] defaultTempFolder = new String[1];
		defaultTempFolder[0] = this.tempFolderPath;
		tempFolder.setDefaultValues(defaultTempFolder);
		tempFolder.setRequired(true);
		configs.add(tempFolder);

		ConfigurationRequirementInteger inputRowLimit = new ConfigurationRequirementInteger(BinderDatabaseAlgorithm.Identifier.INPUT_ROW_LIMIT.name());
		Integer[] defaultInputRowLimit = {this.inputRowLimit};
		inputRowLimit.setDefaultValues(defaultInputRowLimit);
		inputRowLimit.setRequired(false);
		configs.add(inputRowLimit);

		ConfigurationRequirementInteger maxNaryLevel = new ConfigurationRequirementInteger(BinderDatabaseAlgorithm.Identifier.MAX_NARY_LEVEL.name());
		Integer[] defaultMaxNaryLevel = {this.maxNaryLevel};
		maxNaryLevel.setDefaultValues(defaultMaxNaryLevel);
		maxNaryLevel.setRequired(false);
		configs.add(maxNaryLevel);

		ConfigurationRequirementInteger numBucketsPerColumn = new ConfigurationRequirementInteger(BinderDatabaseAlgorithm.Identifier.NUM_BUCKETS_PER_COLUMN.name());
		Integer[] defaultNumBucketsPerColumn = {this.numBucketsPerColumn};
		numBucketsPerColumn.setDefaultValues(defaultNumBucketsPerColumn);
		numBucketsPerColumn.setRequired(true);
		configs.add(numBucketsPerColumn);

		ConfigurationRequirementInteger memoryCheckFrequency = new ConfigurationRequirementInteger(BinderDatabaseAlgorithm.Identifier.MEMORY_CHECK_FREQUENCY.name());
		Integer[] defaultMemoryCheckFrequency = {this.memoryCheckFrequency};
		memoryCheckFrequency.setDefaultValues(defaultMemoryCheckFrequency);
		memoryCheckFrequency.setRequired(true);
		configs.add(memoryCheckFrequency);

		ConfigurationRequirementInteger maxMemoryUsagePercentage = new ConfigurationRequirementInteger(BinderDatabaseAlgorithm.Identifier.MAX_MEMORY_USAGE_PERCENTAGE.name());
		Integer[] defaultMaxMemoryUsagePercentage = {this.maxMemoryUsagePercentage};
		maxMemoryUsagePercentage.setDefaultValues(defaultMaxMemoryUsagePercentage);
		maxMemoryUsagePercentage.setRequired(true);
		configs.add(maxMemoryUsagePercentage);
		
		ConfigurationRequirementBoolean cleanTemp = new ConfigurationRequirementBoolean(BinderDatabaseAlgorithm.Identifier.CLEAN_TEMP.name());
		Boolean[] defaultCleanTemp = new Boolean[1];
		defaultCleanTemp[0] = this.cleanTemp;
		cleanTemp.setDefaultValues(defaultCleanTemp);
		cleanTemp.setRequired(true);
		configs.add(cleanTemp);
		
		ConfigurationRequirementBoolean detectNary = new ConfigurationRequirementBoolean(BinderDatabaseAlgorithm.Identifier.DETECT_NARY.name());
		Boolean[] defaultDetectNary = new Boolean[1];
		defaultDetectNary[0] = this.detectNary;
		detectNary.setDefaultValues(defaultDetectNary);
		detectNary.setRequired(true);
		configs.add(detectNary);

		ConfigurationRequirementBoolean filterKeyForeignkeys = new ConfigurationRequirementBoolean(BinderDatabaseAlgorithm.Identifier.FILTER_KEY_FOREIGNKEYS.name());
		Boolean[] defaultFilterKeyForeignkeys = new Boolean[1];
		defaultFilterKeyForeignkeys[0] = this.filterKeyForeignkeys;
		filterKeyForeignkeys.setDefaultValues(defaultFilterKeyForeignkeys);
		filterKeyForeignkeys.setRequired(true);
		configs.add(filterKeyForeignkeys);
		
		return configs;
	}


	@Override
	public void setTableInputConfigurationValue(String identifier, TableInputGenerator... values) throws AlgorithmConfigurationException {
		if (BinderDatabaseAlgorithm.Identifier.INPUT_DATABASE.name().equals(identifier)) {
			this.tableInputGenerator = asList(values);
		} else
			this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
	}

	@Override
	public void setResultReceiver(InclusionDependencyResultReceiver resultReceiver) {
		this.resultReceiver = resultReceiver;
	}

	@Override
	public void setIntegerConfigurationValue(String identifier, Integer... values) throws AlgorithmConfigurationException {
		if (BinderDatabaseAlgorithm.Identifier.INPUT_ROW_LIMIT.name().equals(identifier)) {
			if (values.length > 0)
				this.inputRowLimit = values[0];
		}
		else if (BinderDatabaseAlgorithm.Identifier.MAX_NARY_LEVEL.name().equals(identifier)) {
			if (values.length > 0)
				this.maxNaryLevel = values[0];
		}
		else if (BinderDatabaseAlgorithm.Identifier.NUM_BUCKETS_PER_COLUMN.name().equals(identifier)) {
			if (values[0] <= 0)
				throw new AlgorithmConfigurationException(BinderDatabaseAlgorithm.Identifier.NUM_BUCKETS_PER_COLUMN.name() + " must be greater than 0!");
			this.numBucketsPerColumn = values[0];
		}
		else if (BinderDatabaseAlgorithm.Identifier.MEMORY_CHECK_FREQUENCY.name().equals(identifier)) {
			if (values[0] <= 0)
				throw new AlgorithmConfigurationException(BinderDatabaseAlgorithm.Identifier.MEMORY_CHECK_FREQUENCY.name() + " must be greater than 0!");
			this.memoryCheckFrequency = values[0];
		}
		else if (BinderDatabaseAlgorithm.Identifier.MAX_MEMORY_USAGE_PERCENTAGE.name().equals(identifier)) {
			if (values[0] <= 0)
				throw new AlgorithmConfigurationException(BinderDatabaseAlgorithm.Identifier.MAX_MEMORY_USAGE_PERCENTAGE.name() + " must be greater than 0!");
			this.maxMemoryUsagePercentage = values[0];
		}
		else 
			this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
	}

	@Override
	public void setStringConfigurationValue(String identifier, String... values) throws AlgorithmConfigurationException {
		if (BinderDatabaseAlgorithm.Identifier.DATABASE_NAME.name().equals(identifier))
			this.databaseName = values[0];
		else if (BinderDatabaseAlgorithm.Identifier.DATABASE_TYPE.name().equals(identifier)) {
			if (BinderDatabaseAlgorithm.Database.MYSQL.name().equals(values[0]))
				this.dao = new MySQLDataAccessObject();
			else if (BinderDatabaseAlgorithm.Database.DB2.name().equals(values[0]))
				this.dao = new DB2DataAccessObject();
			else if (BinderDatabaseAlgorithm.Database.POSTGRESQL.name().equals(values[0]))
				this.dao = new PostgreSQLDataAccessObject();
			else
				this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
		}
//		else if (BinderDatabaseAlgorithm.Identifier.INPUT_TABLES.name().equals(identifier))
//			this.tableNames = values;
		else if (BinderDatabaseAlgorithm.Identifier.TEMP_FOLDER_PATH.name().equals(identifier)) {
			if ("".equals(values[0]) || " ".equals(values[0]) || "/".equals(values[0]) || "\\".equals(values[0]) || File.separator.equals(values[0]) || FileUtils.isRoot(new File(values[0])))
				throw new AlgorithmConfigurationException(BinderDatabaseAlgorithm.Identifier.TEMP_FOLDER_PATH + " must not be \"" + values[0] + "\"");
			this.tempFolderPath = values[0];
		}
		else
			this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
	}
	
	@Override
	public void setBooleanConfigurationValue(String identifier, Boolean... values) throws AlgorithmConfigurationException {
		if (BinderDatabaseAlgorithm.Identifier.CLEAN_TEMP.name().equals(identifier))
			this.cleanTemp = values[0];
		else if (BinderDatabaseAlgorithm.Identifier.DETECT_NARY.name().equals(identifier))
			this.detectNary = values[0];
		else if (BinderDatabaseAlgorithm.Identifier.FILTER_KEY_FOREIGNKEYS.name().equals(identifier))
			this.filterKeyForeignkeys = values[0];
		else
			this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
	}

	private void handleUnknownConfiguration(String identifier, String value) throws AlgorithmConfigurationException {
		throw new AlgorithmConfigurationException("Unknown configuration: " + identifier + " -> " + value);
	}
	
	@Override
	public void execute() throws AlgorithmExecutionException {
		super.execute();
	}

	@Override
	public String getAuthors() {
		return this.getAuthorName();
	}

	@Override
	public String getDescription() {
		return this.getDescriptionText();
	}
}
