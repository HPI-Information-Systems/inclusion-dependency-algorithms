package de.metanome.algorithms.mind2.utils;

import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

public class CurrentIterator<T> extends AbstractIterator<T> {

    private Iterator<T> iter;
    private T current;

    public CurrentIterator(Iterable<T> iter) {
        this.iter = iter.iterator();
        next();
    }

    @Override
    protected T computeNext() {
        if (iter.hasNext()) {
            T result = current;
            current = iter.next();
            return result;
        }
        return endOfData();
    }

    public T current() {
        return current;
    }
}
