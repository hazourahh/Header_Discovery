package de.uni_potsdam.hpi.table_header.data_structures.Result;

public class Header_Candidate extends Candidate {
    private String header;


    public Header_Candidate(String header, double similarity_score) {
        super(similarity_score);
        this.header = header;

    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }


    @Override
    public String toString() {
        return "("+header+","+getSimilarity_score()+")";
    }
}
