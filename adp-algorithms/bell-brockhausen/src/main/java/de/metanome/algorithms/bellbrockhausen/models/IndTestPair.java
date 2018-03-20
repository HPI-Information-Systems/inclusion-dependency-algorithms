package de.metanome.algorithms.bellbrockhausen.models;

import lombok.Data;

@Data
public class IndTestPair {

    public static IndTestPair fromAttributes(Attribute baseCandidate, Attribute iterateCandidate) {
        return new IndTestPair(
                IndTest.fromAttributes(baseCandidate, iterateCandidate),
                IndTest.fromAttributes(iterateCandidate, baseCandidate));
    }

    private final IndTest fromBase; // A_i <= A_i+r
    private final IndTest toBase; // A_i+r <= A_i
}
