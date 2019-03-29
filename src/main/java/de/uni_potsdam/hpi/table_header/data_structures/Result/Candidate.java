package de.uni_potsdam.hpi.table_header.data_structures.Result;

/**
 * @author Hazar Harmouch
 */
public abstract class Candidate {

    private double similarity_score;

    public Candidate(double similarity_score) {
        this.similarity_score = similarity_score;
    }

    public double getSimilarity_score() {
        return similarity_score;
    }

    public void setSimilarity_score(double similarity_score) {
        this.similarity_score = similarity_score;
    }
}
