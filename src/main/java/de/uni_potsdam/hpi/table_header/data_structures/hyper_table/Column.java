package de.uni_potsdam.hpi.table_header.data_structures.hyper_table;


import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import de.uni_potsdam.hpi.table_header.io.Config;


import java.io.Serializable;

/**
 * @author Hazar.Harmouch
 * Represent a Column in the HTable
 */


public class Column implements Serializable {

    private String Label; /* Column header */
    private HyperLogLogPlus Values;/* a Hyperloglog representation of the column values */



    /**
     * Constructor
     *
     * @param label   header of the column
     */
    Column(String label) {
        super();
        Label = label;
        //Values = new HyperLogLog(HLLsize);
        Values = new HyperLogLogPlus(Config.HLL_PLUS_P,Config.HLL_PLUS_SP);
    }

    /**
     * @return the header of the column
     */
    public String getLabel() {
        return Label;
    }

    /**
     * @return the hyperloglog representation of the column
     */
    public HyperLogLogPlus getValues() {return Values;}

//--------------------------------------------------------------------------

    /**
     * add a value to the hyperloglog
     *
     * @param value a new value of the column
     */
    void addValue(Object value) {

       Values.offer(value);

    }

    /**
     * @return the number of distinct values in the column
     */

    public long cardinality() {
        return Values.cardinality();
    }

}
