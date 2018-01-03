package de.metanome.util;

import de.metanome.algorithm_integration.AlgorithmExecutionException;

import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkState;

/**
 * Checked version of the {@link com.google.common.base.AbstractIterator}.
 * Also provides access to the current element.
 * @param <T>
 */
public abstract class CheckedAbstractIterator<T> {
    private State state = State.NOT_READY;

    protected CheckedAbstractIterator() {}

    private enum State {
        READY,
        NOT_READY,
        DONE,
        FAILED,
    }

    private T next;
    private T current;

    protected abstract T computeNext() throws AlgorithmExecutionException;

    protected final T endOfData() {
        state = State.DONE;
        return null;
    }

    public final boolean hasNext() throws AlgorithmExecutionException {
        checkState(state != State.FAILED);
        switch (state) {
            case READY:
                return true;
            case DONE:
                return false;
            default:
        }
        return tryToComputeNext();
    }

    private boolean tryToComputeNext() throws AlgorithmExecutionException {
        state = State.FAILED; // temporary pessimism
        next = computeNext();
        if (state != State.DONE) {
            state = State.READY;
            return true;
        }
        return false;
    }

    public final T next() throws AlgorithmExecutionException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        state = State.NOT_READY;
        current = next;
        next = null;
        return current;
    }

    public final T current() {
        return current;
    }
}
