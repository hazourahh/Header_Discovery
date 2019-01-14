package de.uni_potsdam.hpi.table_header.data_structures.statistics_db;

import com.clearspring.analytics.stream.membership.BloomFilter;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ACSDb implements Serializable {

    /***
     * Statistics about general WebTabeles schema use.
     */

    private ArrayList<Schema_statistic> db = new ArrayList<>();
    private Map<String, Integer> frequency_cash;
    private int TOTAL_SUM_ALL_COUNT = 0;   // to calculate probability

    //TODO: you cann add other schemata from another sources


    public ACSDb() {
        frequency_cash=new HashMap<>();
    }



    public void addSchema(String schema, int occuerance) {
        //TODO: add an preprocessing here

       Schema_statistic new_sch=new Schema_statistic(schema,occuerance);
            db.add(new_sch);
            TOTAL_SUM_ALL_COUNT += occuerance;

    }

    /***
     *
     * @param header
     * @return return the number of tables this header have been seen in
     */
    public int get_header_frequency(String header)
    {  // fetch the frequency if already see this header
        if(frequency_cash.containsKey(header))
        return frequency_cash.get(header);
     else {
         //caculate frequency
            int freq=1; // for  smoothing

            freq+=  db.stream()
                 // .filter(schema -> schema.getKey().is_combo()) // combo
                 .filter(schema -> schema.containsHeader(header)) //refers to the required header
                 .mapToInt(schema -> schema.getCount())
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
        return  freq+db.stream()
               // .filter(schema -> schema.getKey().is_combo()) //combo
                .filter(schema -> schema.containsHeader(A) && schema.containsHeader(B)) //has both neaders
                .mapToDouble(schema -> schema.getCount())
                .sum();
    }

    /***
     * @param schema
     * @return the schema coherency score which is the average of PMI scores for every pair of distinct attributes A and B
     */
    public double cohere(List<String> schema) {
        double totalPMI = 0;

        //TODO: check this code optimality
        if(schema.size()>1) {
            for (int i = 0; i < schema.size(); i++) {
                String A = schema.get(i);
                for (int j = i + 1; j < schema.size(); j++) {
                    String B = schema.get(j);
                    if (!A.equals(B)) totalPMI += pmi(A, B);
                }
            }
            return totalPMI / (schema.size() + 1 * (schema.size()));
        }
        return 0;
    }

    /***
     *
     * @param X
     * @param Y
     * @return the Positive Pointwise Mutual Information (PMI) which gives a sense of how strongly two items are related
     */

    public double pmi(String X, String Y) {
  return Math.max(0,((Math.log(get_header_pair_frequency(X,Y)*TOTAL_SUM_ALL_COUNT)/(get_header_frequency(X)*get_header_frequency(Y)))/Math.log(2)));
    }





}
