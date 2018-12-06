package de.uni_potsdam.hpi.table_header.data_structures.Result;

import java.util.ArrayList;

public class Header_Candidate {
    private String header;
    private  double similarity_score;

    public Header_Candidate(String header, double similarity_score) {
        this.header = header;
        this.similarity_score = similarity_score;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    double getSimilarity_score() {
        return similarity_score;
    }

    public void setSimilarity_score(double similarity_score) {
        this.similarity_score = similarity_score;
    }

    @Override
    public String toString() {
        return "("+header+","+similarity_score+")";
    }
}
