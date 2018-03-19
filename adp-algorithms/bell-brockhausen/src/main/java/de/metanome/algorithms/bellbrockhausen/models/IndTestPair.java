package de.metanome.algorithms.bellbrockhausen.models;

import lombok.Data;

@Data
public class IndTestPair {

    public static IndTestPair fromAttributes(Attribute candidateA, Attribute candidateB) {
        return new IndTestPair(
                IndTest.fromAttributes(candidateA, candidateB), IndTest.fromAttributes(candidateB, candidateA));
    }

    private final IndTest to; // A_i <= A_i+r
    private final IndTest from; // A_i+r <= A_i
}
