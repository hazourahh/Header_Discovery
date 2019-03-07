package de.uni_potsdam.hpi.table_header.data_structures.Result;

import java.util.List;

/**
 * @author Hazar Harmouch
 */

public class Schema_Candidate extends Candidate {

    private List<String> schema;

    public Schema_Candidate(List<String> schema, double similarity_score) {
        super(similarity_score);
        this.schema = schema;

    }

    public List<String> getSchema() {
        return schema;
    }

    public void setSchema(List<String> schema) {
        this.schema = schema;
    }

    @Override
    public String toString() {
        return "(" + schema + "," + getSimilarity_score() + ")";
    }
}
