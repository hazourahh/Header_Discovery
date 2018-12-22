package de.uni_potsdam.hpi.table_header.data_structures.statistics_db;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ACSDb implements Serializable {

    /***
     * Statistics about general WebTabeles schema use.
     */

    private Map<List<String>, Integer> db;
    private Map<String, Integer> frequency_cash;
    private int TOTAL_SUM_ALL_COUNT = 0;   // to calculate probability

    //TODO: you cann add other schemata from another sources


    public ACSDb() {
        db = new HashMap<>();
        frequency_cash=new HashMap<>();

    }



    public void addSchema(String schema, int occuerance) {
        //TODO: add an preprocessing here

        if(!schema.equals("")) {
            List<String> temp=Arrays.stream(schema.split("_"))
                    .map(e -> e.replaceAll("[\\]\\[(){},.;:!?<>%\\-*]", " "))
                    .map(String::trim)
                    .map(String::toLowerCase)
                    .collect(Collectors.toList());
            db.put(temp, occuerance);
            TOTAL_SUM_ALL_COUNT += occuerance;
        }

    }

    /***
     *
     * @param header
     * @return return the number of tables this header have been seen in
     */
    public int get_header_frequency(String header)
    {  // fetch the frequency if already say this header
        if(frequency_cash.containsKey(header))
        return frequency_cash.get(header);
     else {
         //caculate frequency
            int freq=1; // for  smothing
            freq+=  db.entrySet().stream()
                 // .filter(schema -> schema.getKey().is_combo()) // combo
                 .filter(schema -> schema.getKey().contains(header)) //refers to the required header
                 .mapToInt(schema -> schema.getValue())
                 .sum();
         //cash
         frequency_cash.put(header,freq);
        return freq;

    }
    }


    /***
     *
     * @param A
     * @param B
     * @return return the number of tables this header have been seen in
     */
    public double get_header_pair_frequency(String A, String B)
    { int freq=1;
        return  freq+db.entrySet().stream()
               // .filter(schema -> schema.getKey().is_combo()) //combo
                .filter(schema -> schema.getKey().contains(A) && schema.getKey().contains(B)) //has both neaders
                .mapToDouble(schema -> schema.getValue())
                .sum();
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
  return ((Math.log(get_header_pair_frequency(X,Y)*TOTAL_SUM_ALL_COUNT)/(get_header_frequency(X)*get_header_frequency(Y)))/Math.log(2));
    }





}
