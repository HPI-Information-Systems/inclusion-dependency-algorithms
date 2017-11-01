package de.metanome.algorithms.reindeer;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.BooleanParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.IntegerParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.RelationalInputParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.StringParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.TableInputParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementBoolean;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementFileInput;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementInteger;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementString;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementTableInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;

public class ReINDeer extends ReINDeerAlgorithm implements InclusionDependencyAlgorithm, // Output
		RelationalInputParameterAlgorithm, TableInputParameterAlgorithm, // Input
		IntegerParameterAlgorithm, StringParameterAlgorithm, BooleanParameterAlgorithm { // Parameter

	public enum Identifier {
		INPUT_FILES, INPUT_TABLES, TEMP_FOLDER_PATH, CLEAN_TEMP, DETECT_NARY, MAX_NARY_LEVEL, FILTER_KEY_FOREIGNKEYS
	}

	@Override
	public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
		ConfigurationRequirementFileInput fileInput = new ConfigurationRequirementFileInput(
				ReINDeer.Identifier.INPUT_TABLES.name(), ConfigurationRequirement.ARBITRARY_NUMBER_OF_VALUES);
		
		ConfigurationRequirementTableInput tableInput = new ConfigurationRequirementTableInput(
				ReINDeer.Identifier.INPUT_TABLES.name(), ConfigurationRequirement.ARBITRARY_NUMBER_OF_VALUES);

		ConfigurationRequirementString tempFolder = new ConfigurationRequirementString(
				ReINDeer.Identifier.TEMP_FOLDER_PATH.name());
		String[] defaultTempFolder = { "ReINDeer_temp" };
		tempFolder.setDefaultValues(defaultTempFolder);
		tempFolder.setRequired(true);

		ConfigurationRequirementBoolean cleanTemp = new ConfigurationRequirementBoolean(
				ReINDeer.Identifier.CLEAN_TEMP.name());
		Boolean[] defaultCleanTemp = { Boolean.valueOf(true) };
		cleanTemp.setDefaultValues(defaultCleanTemp);
		cleanTemp.setRequired(true);

		ConfigurationRequirementBoolean detectNary = new ConfigurationRequirementBoolean(
				ReINDeer.Identifier.DETECT_NARY.name());
		Boolean[] defaultDetectNary = { Boolean.valueOf(true) };
		detectNary.setDefaultValues(defaultDetectNary);
		detectNary.setRequired(true);

		ConfigurationRequirementInteger maxNaryLevel = new ConfigurationRequirementInteger(
				ReINDeer.Identifier.MAX_NARY_LEVEL.name());
		Integer[] defaultMaxNaryLevel = { Integer.valueOf(-1) };
		maxNaryLevel.setDefaultValues(defaultMaxNaryLevel);
		maxNaryLevel.setRequired(true);

		ConfigurationRequirementBoolean filterKeyForeignkeys = new ConfigurationRequirementBoolean(
				ReINDeer.Identifier.FILTER_KEY_FOREIGNKEYS.name());
		Boolean[] defaultFilterKeyForeignkeys = { Boolean.valueOf(false) };
		filterKeyForeignkeys.setDefaultValues(defaultFilterKeyForeignkeys);
		filterKeyForeignkeys.setRequired(true);

		return new ArrayList<>(Arrays.asList(fileInput, tableInput, tempFolder, cleanTemp, detectNary, maxNaryLevel,
				filterKeyForeignkeys));
	}

	@Override
	public void setRelationalInputConfigurationValue(String identifier, RelationalInputGenerator... values)
			throws AlgorithmConfigurationException {
		if (ReINDeer.Identifier.INPUT_FILES.name().equals(identifier))
			this.relationalInputGenerators = values;
		else
			this.handleUnknownConfiguration(identifier, values);
	}
	
	@Override
	public void setTableInputConfigurationValue(String identifier, TableInputGenerator... values)
			throws AlgorithmConfigurationException {
		if (ReINDeer.Identifier.INPUT_TABLES.name().equals(identifier))
			this.tableInputGenerators = values;
		else
			this.handleUnknownConfiguration(identifier, values);
	}

	@Override
	public void setResultReceiver(InclusionDependencyResultReceiver resultReceiver) {
		this.resultReceiver = resultReceiver;
	}

	@Override
	public void setIntegerConfigurationValue(String identifier, Integer... values)
			throws AlgorithmConfigurationException {
		if (ReINDeer.Identifier.MAX_NARY_LEVEL.name().equals(identifier))
			this.maxNaryLevel = values[0].intValue();
		else
			this.handleUnknownConfiguration(identifier, values);
	}

	@Override
	public void setStringConfigurationValue(String identifier, String... values)
			throws AlgorithmConfigurationException {
		if (ReINDeer.Identifier.TEMP_FOLDER_PATH.name().equals(identifier)) {
			if (this.isValidPath(values[0]))
				throw new AlgorithmConfigurationException(ReINDeer.Identifier.TEMP_FOLDER_PATH + " must not be \"" + values[0] + "\"");
			this.tempFolderPath = values[0] + File.separator + "buckets" + File.separator;
		} else
			this.handleUnknownConfiguration(identifier, values);
	}

	@Override
	public void setBooleanConfigurationValue(String identifier, Boolean... values)
			throws AlgorithmConfigurationException {
		if (ReINDeer.Identifier.CLEAN_TEMP.name().equals(identifier))
			this.cleanTemp = values[0].booleanValue();
		else if (ReINDeer.Identifier.DETECT_NARY.name().equals(identifier))
			this.detectNary = values[0].booleanValue();
		else if (ReINDeer.Identifier.FILTER_KEY_FOREIGNKEYS.name().equals(identifier))
			this.filterKeyForeignkeys = values[0].booleanValue();
		else
			this.handleUnknownConfiguration(identifier, values);
	}

	private void handleUnknownConfiguration(String identifier, Object[] values) throws AlgorithmConfigurationException {
		throw new AlgorithmConfigurationException("Unknown configuration: " + identifier + " -> " + this.concat(values, ","));
	}
	
	private String concat(Object[] objects, String separator) {
		if (objects == null)
			return "";
		
		StringBuilder buffer = new StringBuilder();
		
		for (int i = 0; i < objects.length; i++) {
			buffer.append(objects[i].toString());
			if ((i + 1) < objects.length)
				buffer.append(separator);
		}
		
		return buffer.toString();
	}
	
	private boolean isValidPath(String path) {
		// Path must be valid
		if (path == null || path.equals("") || path.equals(" ") || path.equals("/") || path.equals("\\") || path.equals(File.separator))
			return false;
		
		// Path must not be root
		File file = new File(path);
		File rootSlash = new File("/");
		File rootBackslash = new File("\\");
		if (file.getAbsolutePath().equals(rootSlash.getAbsolutePath()) || file.getAbsolutePath().equals(rootBackslash.getAbsolutePath()))
			return false;		
		
		return true;
	}

	@Override
	public void execute() throws AlgorithmExecutionException {
		super.execute();
	}

	@Override
	public String getAuthors() {
		return "";
	}

	@Override
	public String getDescription() {
		return "";
	}

}
