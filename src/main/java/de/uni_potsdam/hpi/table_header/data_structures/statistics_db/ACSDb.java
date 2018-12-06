package de.uni_potsdam.hpi.table_header.data_structures.statistics_db;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ACSDb implements Serializable {

    private Map<WT_Schema, Integer> db;
    //TODO: you cann add other schemata from another sources


    public ACSDb() { db = new HashMap<>(); }


    public void addSchema(WT_Schema wts, int occuerance) { db.put(wts, occuerance); }

    public double calculate_Conditional_Probability(String header1, String header2) {

        //TODO: implement this
        return 0; }


}
