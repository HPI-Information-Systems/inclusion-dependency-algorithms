package de.metanome.algorithms.binder;

import de.metanome.algorithms.binder.structures.IntSingleLinkedList;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * Created by maxi on 07.01.18.
 */
public class FetchedCandidates {
    Int2ObjectOpenHashMap<IntSingleLinkedList> dep2refToCheck;
    Int2ObjectOpenHashMap<IntSingleLinkedList> dep2refFinal;

    public FetchedCandidates(Int2ObjectOpenHashMap<IntSingleLinkedList> dep2refToCheck, Int2ObjectOpenHashMap<IntSingleLinkedList> dep2refFinal) {
        this.dep2refToCheck = dep2refToCheck;
        this.dep2refFinal = dep2refFinal;
    }

    public Int2ObjectOpenHashMap<IntSingleLinkedList> getDep2refToCheck() {
        return dep2refToCheck;
    }

    public void setDep2refToCheck(Int2ObjectOpenHashMap<IntSingleLinkedList> dep2refToCheck) {
        this.dep2refToCheck = dep2refToCheck;
    }

    public Int2ObjectOpenHashMap<IntSingleLinkedList> getDep2refFinal() {
        return dep2refFinal;
    }

    public void setDep2refFinal(Int2ObjectOpenHashMap<IntSingleLinkedList> dep2refFinal) {
        this.dep2refFinal = dep2refFinal;
    }

    public Int2ObjectOpenHashMap<IntSingleLinkedList> setDep2refToCheck(int candidateId, IntSingleLinkedList candidate) {
        this.dep2refToCheck.put(candidateId, candidate);
        return this.dep2refToCheck;
    }

    public Int2ObjectOpenHashMap<IntSingleLinkedList> setDep2refFinal(int candidateId, IntSingleLinkedList candidate) {
        this.dep2refFinal.put(candidateId, candidate);
        return this.dep2refFinal;
    }
}
