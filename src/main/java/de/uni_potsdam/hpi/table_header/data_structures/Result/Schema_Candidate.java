package de.uni_potsdam.hpi.table_header.data_structures.Result;

import java.util.List;

public class Schema_Candidate extends Candidate {

    private List<String> schema;


    public Schema_Candidate(List<String> schema, double similarity_score) {
        super(similarity_score);
        this.schema= schema;

    }

    public List<String> getHeader() {
        return schema;
    }

    public void setHeader(List<String> schema) {
        this.schema = schema;
    }


    @Override
    public String toString() {
        return "("+schema+","+getSimilarity_score()+")";
    }
}
