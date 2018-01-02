package de.metanome.algorithms.mind2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_execution.FileGenerator;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.TableInputParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.TempFileAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementRelationalInput;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.mind2.configuration.ConfigurationKey;
import de.metanome.algorithms.mind2.configuration.Mind2Configuration;
import de.metanome.algorithms.mind2.configuration.Mind2Configuration.Mind2ConfigurationBuilder;
import de.metanome.input.ind.InclusionDependencyInput;
import de.metanome.input.ind.InclusionDependencyInputConfigurationRequirements;
import de.metanome.input.ind.InclusionDependencyInputGenerator;
import de.metanome.input.ind.InclusionDependencyInputParameterAlgorithm;
import de.metanome.input.ind.InclusionDependencyParameters;

import java.util.ArrayList;

public class Mind2Algorithm implements InclusionDependencyAlgorithm, TableInputParameterAlgorithm,TempFileAlgorithm,
        InclusionDependencyInputParameterAlgorithm{

    private final Mind2ConfigurationBuilder configurationBuilder;
    private final InclusionDependencyParameters uindParams = new InclusionDependencyParameters();

    public Mind2Algorithm() {
       configurationBuilder = Mind2Configuration.builder();
    }

    @Override
    public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
        ArrayList<ConfigurationRequirement<?>> requirements = new ArrayList<>();
        requirements.add(new ConfigurationRequirementRelationalInput(
                ConfigurationKey.TABLE.name(), ConfigurationRequirement.ARBITRARY_NUMBER_OF_VALUES));
        requirements.addAll(InclusionDependencyInputConfigurationRequirements.indInput());
        return requirements;
    }

    @Override
    public void setTableInputConfigurationValue(String identifier, TableInputGenerator... values) {
        if (identifier.equals(ConfigurationKey.TABLE.name()) && values.length > 0) {
            configurationBuilder.inputGenerators(ImmutableList.copyOf(values));
            InclusionDependencyInputConfigurationRequirements
                    .acceptTableInputGenerator(values, uindParams);
        }
    }

    @Override
    public void setTempFileGenerator(FileGenerator tempFileGenerator) {
        configurationBuilder.tempFileGenerator(tempFileGenerator);
    }

    @Override
    public void setResultReceiver(InclusionDependencyResultReceiver resultReceiver) {
        configurationBuilder.resultReceiver(resultReceiver);
    }

    @Override
    public void setListBoxConfigurationValue(String identifier, String... selectedValues) {
        InclusionDependencyInputConfigurationRequirements.acceptListBox(identifier, selectedValues, uindParams);
    }

    @Override
    public void setStringConfigurationValue(String identifier, String... values) {
        InclusionDependencyInputConfigurationRequirements.acceptString(identifier, values, uindParams);
    }

    @Override
    public void execute() throws AlgorithmExecutionException {
        InclusionDependencyInput uindInput = new InclusionDependencyInputGenerator().get(uindParams);
        ImmutableSet<InclusionDependency> uinds = ImmutableSet.copyOf(uindInput.execute());
        Mind2Configuration conifg = configurationBuilder.unaryInds(uinds).build();
        new Mind2(conifg).execute();
    }

    @Override
    public String getAuthors() {
        return "Nils Strelow, Fabian Windheuser";
    }

    @Override
    public String getDescription() {
        return "Implementation of 'Detecting Maximum Inclusion Dependencies without Candidate Generation' " +
                "by Shaabani, Meinel, 2016";
    }
}
