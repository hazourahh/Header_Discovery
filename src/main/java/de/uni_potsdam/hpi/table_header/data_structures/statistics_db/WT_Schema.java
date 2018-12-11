package de.uni_potsdam.hpi.table_header.data_structures.statistics_db;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class WT_Schema implements Serializable {
    // if it is combo: the entry indicates that a schema with exactly WT_SCHEMA.getSchemaasList attributes was seen in x different tables
    //else: the entry indicates that the attribute WT_SCHEMA.getSchema was seen in x different tables

    private boolean is_combo;
    private String schema;

    public WT_Schema(String is_combo, String schema) {
        this.is_combo = is_combo.equals("combo");
        this.schema = schema;
    }

    public boolean is_combo() {
        return is_combo;
    }

    public String getSchema() {
        return schema;
    }


    public List<String> getSchemaasList()
    {return Arrays.stream(schema.split("_"))
            .map(e->e.replace("-"," "))
            .map(String::trim)
            .collect(Collectors.toList());}

    @Override
    public String toString() {
        return schema ;
    }
}
