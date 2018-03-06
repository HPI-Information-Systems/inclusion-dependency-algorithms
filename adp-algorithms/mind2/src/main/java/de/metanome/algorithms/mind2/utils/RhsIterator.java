package de.metanome.algorithms.mind2.utils;

import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

public class RhsIterator extends AbstractIterator<Integer> {

    private final Integer uindId;
    private Iterator<Integer> iter;
    private Integer current;

    public RhsIterator(Integer uindId, Iterable<Integer> iter) {
        this.uindId = uindId;
        this.iter = iter.iterator();
        next();
    }

    @Override
    protected Integer computeNext() {
        if (iter.hasNext()) {
            Integer result = current;
            current = iter.next();
            return result;
        }
        return endOfData();
    }

    public Integer current() {
        return current;
    }

    public Integer getUindId() {
        return uindId;
    }
}
