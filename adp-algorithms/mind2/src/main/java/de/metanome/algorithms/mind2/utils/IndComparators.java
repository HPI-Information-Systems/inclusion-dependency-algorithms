package de.metanome.algorithms.mind2.utils;

import de.metanome.algorithms.mind2.model.UindCoordinates;

import java.util.Comparator;

public class IndComparators {

    public static class UindCoordinatesReaderComparator implements Comparator<UindCoordinatesReader> {

        private final UindCoordinatesComparator uindCoordinatesComparator = new UindCoordinatesComparator();

        @Override
        public int compare(UindCoordinatesReader readerA, UindCoordinatesReader readerB) {
            return uindCoordinatesComparator.compare(readerA.current(), readerB.current());
        }
    }

    public static class UindCoordinatesComparator implements Comparator<UindCoordinates> {
        @Override
        public int compare(UindCoordinates coordinatesA, UindCoordinates coordinatesB) {
            return coordinatesA.getLhsIndex() - coordinatesB.getLhsIndex();
        }
    }

    public static class RhsComrapator implements Comparator<RhsIterator> {
        @Override
        public int compare(RhsIterator iterA, RhsIterator iterB) {
            return iterA.current() - iterB.current();
        }
    }
}
