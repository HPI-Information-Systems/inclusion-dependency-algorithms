package de.metanome.algorithms.binder.io;

import java.util.List;

import de.metanome.algorithm_integration.input.InputIterationException;

public interface InputIterator extends AutoCloseable {

	boolean next() throws InputIterationException;
	String getValue(int columnIndex) throws InputIterationException;
	List<String> getValues() throws InputIterationException;
}
