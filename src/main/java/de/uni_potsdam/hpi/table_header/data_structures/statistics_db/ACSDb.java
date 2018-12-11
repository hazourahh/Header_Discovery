package de.uni_potsdam.hpi.table_header.data_structures.statistics_db;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ACSDb implements Serializable {

    /***
     * Statistics about general WebTabeles schema use.
     */

    private Map<WT_Schema, Integer> db;
    private int TOTAL_SUM_ALL_COUNT = 0;   // to calculate probability
    //TODO: you cann add other schemata from another sources


    public ACSDb() {
        db = new HashMap<>();
    }


    public void addSchema(WT_Schema wts, int occuerance) {
        //TODO: check if we need the single attributes
        if (wts.is_combo()) {
            db.put(wts, occuerance);
            TOTAL_SUM_ALL_COUNT += occuerance;
        }
    }

    /***
     *
     * @param header
     * @return return the number of tables this header have been seen in
     */
    public int get_header_frequency(String header)
    { return
       db.entrySet().stream()
            .filter(schema -> schema.getKey().is_combo()) // combo
            .filter(schema -> schema.getKey().getSchemaasList().contains(header)) //refers to the required header
            .mapToInt(schema -> schema.getValue())
            .sum();


    }


    /***
     *
     * @param A
     * @param B
     * @return return the number of tables this header have been seen in
     */
    public double get_header_pair_frequency(String A, String B)
    {
        return  db.entrySet().stream()
                .filter(schema -> schema.getKey().is_combo()) //combo
                .filter(schema -> schema.getKey().getSchemaasList().contains(A) && schema.getKey().getSchemaasList().contains(B)) //has both neaders
                .mapToDouble(schema -> schema.getValue())
                .sum();
    }

    /***
     *
     * @param header
     * @return the probability of header
     */
    public int probibility(String header) {
        return get_header_frequency(header)/TOTAL_SUM_ALL_COUNT;
    }

    /***
     *
     * @param A
     * @param B
     * @return p(A|B) of two headers
     */
    public double conditional_probibility(String A,String B) {

        return get_header_pair_frequency(A,B)/get_header_frequency(B);
    }

    /***
     * @param schema
     * @return the schema coherency score which is the average of PMI scores for every pair of distinct attributes A and B
     */
    public double cohere(List<String> schema) {
        double totalPMI = 0;
        //TODO: check this code optimality
        for (int i = 0; i < schema.size(); i++) {
             String A=schema.get(i);
            for (int j = i + 1; j < schema.size(); j++) {
                String B=schema.get(j);
                if (!A.equals(B)) totalPMI+=pmi(A,B);
            }
        }
        return totalPMI / (schema.size() * (schema.size() - 1));
    }

    /***
     *
     * @param X
     * @param Y
     * @return the Pointwise Mutual Information (PMI) which gives a sense of how strongly two items are related
     */
    public double pmi(String X, String Y) {
        return Math.log(conditional_probibility(X,Y) /probibility(X));
    }





}
