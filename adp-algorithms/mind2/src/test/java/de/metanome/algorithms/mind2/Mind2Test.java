package de.metanome.algorithms.mind2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.algorithm_execution.FileGenerator;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.mind2.configuration.Mind2Configuration;
import de.metanome.util.FileGeneratorFake;
import de.metanome.util.InclusionDependencyResultReceiverStub;
import de.metanome.util.RelationalInputGeneratorStub;
import de.metanome.util.Row;
import org.jukito.JukitoRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(JukitoRunner.class)
public class Mind2Test {

    private static final String INDEX_COLUMN = "MIND2_INDEX";

    private InclusionDependencyResultReceiverStub resultReceiver;
    @Inject private Mind2 mind2;

    @Before
    public void setupMocks(Mind2Configuration config) {
        resultReceiver = new InclusionDependencyResultReceiverStub();
        FileGenerator fileGenerator = new FileGeneratorFake();
        when(config.getResultReceiver()).thenReturn(resultReceiver);
        when(config.getTempFileGenerator()).thenReturn(fileGenerator);
        when(config.getIndexColumn()).thenReturn(INDEX_COLUMN);
    }

    @Test
    public void testPaperExample(Mind2Configuration config) throws AlgorithmExecutionException {
        // GIVEN
        ColumnIdentifier a1 = new ColumnIdentifier("R", "A1");
        ColumnIdentifier a2 = new ColumnIdentifier("R", "A2");
        ColumnIdentifier a3 = new ColumnIdentifier("R", "A3");
        ColumnIdentifier a4 = new ColumnIdentifier("R", "A4");
        ColumnIdentifier a5 = new ColumnIdentifier("R", "A5");
        ColumnIdentifier b1 = new ColumnIdentifier("S", "B1");
        ColumnIdentifier b2 = new ColumnIdentifier("S", "B2");
        ColumnIdentifier b3 = new ColumnIdentifier("S", "B3");
        ColumnIdentifier b4 = new ColumnIdentifier("S", "B4");
        ColumnIdentifier b5 = new ColumnIdentifier("S", "B5");
        TableInputGenerator tableRGenerator = mock(TableInputGenerator.class);
        TableInputGenerator tableSGenerator = mock(TableInputGenerator.class);
        RelationalInputGenerator tableR = RelationalInputGeneratorStub.builder()
                .relationName("R")
                .columnNames(ImmutableList.of(INDEX_COLUMN, "A1", "A2", "A3", "A4", "A5"))
                .rows(ImmutableList.of(
                        Row.of("1", "a", "b", "c", "d", "e"),
                        Row.of("2", "f", "g", "i", "j", "k")))
                .build();
        RelationalInputGenerator tableS = RelationalInputGeneratorStub.builder()
                .relationName("S")
                .columnNames(ImmutableList.of(INDEX_COLUMN, "B1", "B2", "B3", "B4", "B5"))
                .rows(ImmutableList.of(
                        Row.of("1", "a", "b", "c", "d", null),
                        Row.of("2", null, null, "c", "d", null),
                        Row.of("3", null, null, "c", "d", "e"),
                        Row.of("4", "f", "g", "i", null, null),
                        Row.of("5", "f", "g", null, "j", "k")))
                .build();
        ImmutableSet<InclusionDependency> unaryInds = ImmutableSet.of(
                toInd(a1, b1), toInd(a2, b2), toInd(a3, b3), toInd(a4, b4), toInd(a5, b5));
        ImmutableSet<InclusionDependency> maximumInds = ImmutableSet.of(
                new InclusionDependency(new ColumnPermutation(a1, a2, a3), new ColumnPermutation(b1, b2, b3)),
                new InclusionDependency(new ColumnPermutation(a1, a2, a4), new ColumnPermutation(b1, b2, b4)),
                new InclusionDependency(new ColumnPermutation(a4, a5), new ColumnPermutation(b4, b5)));

        when(config.getInputGenerators()).thenReturn(ImmutableList.of(tableRGenerator, tableSGenerator));
        when(config.getUnaryInds()).thenReturn(unaryInds);
        when(tableRGenerator.generateNewCopy()).then(in -> tableR.generateNewCopy());
        when(tableSGenerator.generateNewCopy()).then(in -> tableS.generateNewCopy());
        when(config.getSortedRelationalInput(same(tableRGenerator), any())).then(in -> tableR.generateNewCopy());
        when(config.getSortedRelationalInput(same(tableSGenerator), any())).then(in -> tableS.generateNewCopy());

        // WHEN
        mind2.execute();

        // THEN
        assertThat(resultReceiver.getReceivedResults()).containsExactlyInAnyOrder(toArray(maximumInds));
    }

    @Test
    public void testEqualTables(Mind2Configuration config) throws AlgorithmExecutionException {
        // GIVEN
        ColumnIdentifier a1 = new ColumnIdentifier("R", "A1");
        ColumnIdentifier a2 = new ColumnIdentifier("R", "A2");
        ColumnIdentifier a3 = new ColumnIdentifier("R", "A3");
        ColumnIdentifier b1 = new ColumnIdentifier("S", "B1");
        ColumnIdentifier b2 = new ColumnIdentifier("S", "B2");
        ColumnIdentifier b3 = new ColumnIdentifier("S", "B3");
        TableInputGenerator tableRGenerator = mock(TableInputGenerator.class);
        TableInputGenerator tableSGenerator = mock(TableInputGenerator.class);
        RelationalInputGenerator tableR = RelationalInputGeneratorStub.builder()
                .relationName("R")
                .columnNames(ImmutableList.of(INDEX_COLUMN, "A1", "A2", "A3"))
                .rows(ImmutableList.of(
                        Row.of("1", "a", "b", "c"),
                        Row.of("2", "d", "e", "f")))
                .build();
        RelationalInputGenerator tableS = RelationalInputGeneratorStub.builder()
                .relationName("S")
                .columnNames(ImmutableList.of(INDEX_COLUMN, "B1", "B2", "B3"))
                .rows(ImmutableList.of(
                        Row.of("1", "a", "b", "c"),
                        Row.of("2", "d", "e", "f")))
                .build();
        ImmutableSet<InclusionDependency> unaryInds = ImmutableSet.of(
                toInd(a1, b1), toInd(a2, b2), toInd(a3, b3));
        ImmutableSet<InclusionDependency> maximumInds = ImmutableSet.of(
                new InclusionDependency(new ColumnPermutation(a1, a2, a3), new ColumnPermutation(b1, b2, b3)));

        when(config.getInputGenerators()).thenReturn(ImmutableList.of(tableRGenerator, tableSGenerator));
        when(config.getUnaryInds()).thenReturn(unaryInds);
        when(tableRGenerator.generateNewCopy()).then(in -> tableR.generateNewCopy());
        when(tableSGenerator.generateNewCopy()).then(in -> tableS.generateNewCopy());
        when(config.getSortedRelationalInput(same(tableRGenerator), any())).then(in -> tableR.generateNewCopy());
        when(config.getSortedRelationalInput(same(tableSGenerator), any())).then(in -> tableS.generateNewCopy());

        // WHEN
        mind2.execute();

        // THEN
        assertThat(resultReceiver.getReceivedResults()).containsExactlyInAnyOrder(toArray(maximumInds));
    }

    @Test
    public void testNoMaxInds(Mind2Configuration config) throws AlgorithmExecutionException {
        // GIVEN
        ColumnIdentifier a1 = new ColumnIdentifier("R", "A1");
        ColumnIdentifier a2 = new ColumnIdentifier("R", "A2");
        ColumnIdentifier b1 = new ColumnIdentifier("S", "B1");
        ColumnIdentifier b2 = new ColumnIdentifier("S", "B2");
        TableInputGenerator tableRGenerator = mock(TableInputGenerator.class);
        TableInputGenerator tableSGenerator = mock(TableInputGenerator.class);
        RelationalInputGenerator tableR = RelationalInputGeneratorStub.builder()
                .relationName("R")
                .columnNames(ImmutableList.of(INDEX_COLUMN, "A1", "A2"))
                .rows(ImmutableList.of(
                        Row.of("1", "a", "b", "c"),
                        Row.of("2", "d", "e", "f")))
                .build();
        RelationalInputGenerator tableS = RelationalInputGeneratorStub.builder()
                .relationName("S")
                .columnNames(ImmutableList.of(INDEX_COLUMN, "B1", "B2"))
                .rows(ImmutableList.of(
                        Row.of("1", "a", "e"),
                        Row.of("2", "d", "b")))
                .build();
        ImmutableSet<InclusionDependency> unaryInds = ImmutableSet.of(
                toInd(a1, b1), toInd(a2, b2));
        ImmutableSet<InclusionDependency> maximumInds = ImmutableSet.of(
                new InclusionDependency(new ColumnPermutation(a1), new ColumnPermutation(b1)),
                new InclusionDependency(new ColumnPermutation(a2), new ColumnPermutation(b2)));

        when(config.getInputGenerators()).thenReturn(ImmutableList.of(tableRGenerator, tableSGenerator));
        when(config.getUnaryInds()).thenReturn(unaryInds);
        when(tableRGenerator.generateNewCopy()).then(in -> tableR.generateNewCopy());
        when(tableSGenerator.generateNewCopy()).then(in -> tableS.generateNewCopy());
        when(config.getSortedRelationalInput(same(tableRGenerator), any())).then(in -> tableR.generateNewCopy());
        when(config.getSortedRelationalInput(same(tableSGenerator), any())).then(in -> tableS.generateNewCopy());

        // WHEN
        mind2.execute();

        // THEN
        assertThat(resultReceiver.getReceivedResults()).containsExactlyInAnyOrder(toArray(maximumInds));
    }

    @Test
    public void testUnorderedValues(Mind2Configuration config) throws AlgorithmExecutionException {
        // GIVEN
        ColumnIdentifier a1 = new ColumnIdentifier("R", "A1");
        ColumnIdentifier a2 = new ColumnIdentifier("R", "A2");
        ColumnIdentifier a3 = new ColumnIdentifier("R", "A3");
        ColumnIdentifier a4 = new ColumnIdentifier("R", "A4");
        ColumnIdentifier a5 = new ColumnIdentifier("R", "A5");
        ColumnIdentifier b1 = new ColumnIdentifier("S", "B1");
        ColumnIdentifier b2 = new ColumnIdentifier("S", "B2");
        ColumnIdentifier b3 = new ColumnIdentifier("S", "B3");
        ColumnIdentifier b4 = new ColumnIdentifier("S", "B4");
        ColumnIdentifier b5 = new ColumnIdentifier("S", "B5");
        TableInputGenerator tableRGenerator = mock(TableInputGenerator.class);
        TableInputGenerator tableSGenerator = mock(TableInputGenerator.class);
        RelationalInputGenerator tableR = RelationalInputGeneratorStub.builder()
                .relationName("R")
                .columnNames(ImmutableList.of(INDEX_COLUMN, "A1", "A2", "A3", "A4", "A5"))
                .rows(ImmutableList.of(
                        Row.of("2", "a", "b", "c", "d", "e"),
                        Row.of("1", "f", "g", "i", "j", "k")))
                .build();
        RelationalInputGenerator tableS = RelationalInputGeneratorStub.builder()
                .relationName("S")
                .columnNames(ImmutableList.of(INDEX_COLUMN, "B1", "B2", "B3", "B4", "B5"))
                .rows(ImmutableList.of(
                        Row.of("4", "a", "b", "c", "d", null),
                        Row.of("1", null, null, "c", "d", "e"),
                        Row.of("3", null, null, "c", "d", null),
                        Row.of("2", "f", "g", "i", null, null),
                        Row.of("5", "f", "g", null, "j", "k")))
                .build();
        ImmutableSet<InclusionDependency> unaryInds = ImmutableSet.of(
                toInd(a1, b1), toInd(a2, b2), toInd(a3, b3), toInd(a4, b4), toInd(a5, b5));
        ImmutableSet<InclusionDependency> maximumInds = ImmutableSet.of(
                new InclusionDependency(new ColumnPermutation(a1, a2, a3), new ColumnPermutation(b1, b2, b3)),
                new InclusionDependency(new ColumnPermutation(a1, a2, a4), new ColumnPermutation(b1, b2, b4)),
                new InclusionDependency(new ColumnPermutation(a4, a5), new ColumnPermutation(b4, b5)));

        when(config.getInputGenerators()).thenReturn(ImmutableList.of(tableRGenerator, tableSGenerator));
        when(config.getUnaryInds()).thenReturn(unaryInds);
        when(tableRGenerator.generateNewCopy()).then(in -> tableR.generateNewCopy());
        when(tableSGenerator.generateNewCopy()).then(in -> tableS.generateNewCopy());
        when(config.getSortedRelationalInput(same(tableRGenerator), any())).then(in -> tableR.generateNewCopy());
        when(config.getSortedRelationalInput(same(tableSGenerator), any())).then(in -> tableS.generateNewCopy());

        // WHEN
        mind2.execute();

        // THEN
        assertThat(resultReceiver.getReceivedResults()).containsExactlyInAnyOrder(toArray(maximumInds));
    }

    private InclusionDependency toInd(ColumnIdentifier dependant, ColumnIdentifier referenced) {
        return new InclusionDependency(new ColumnPermutation(dependant), new ColumnPermutation(referenced));
    }

    private InclusionDependency[] toArray(Set<InclusionDependency> inds) {
        return inds.stream().toArray(InclusionDependency[]::new);
    }
}
