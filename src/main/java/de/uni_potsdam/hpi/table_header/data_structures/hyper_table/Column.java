package de.uni_potsdam.hpi.table_header.data_structures.hyper_table;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

import java.io.Serializable;
/**
 * @author Hazar.Harmouch
 * Represent a Column in the HTable
 */


public class Column implements Serializable {

    private String Label; /* Column header */
    private HyperLogLog Values;/* a Hyperloglog representation of the column values */

    /**
     *  Constructor
     * @param label header of the column
     * @param HLLsize size of the hyperloglog representaion
     */
    Column(String label, int HLLsize) {
        super();
        Label = label;
        Values=new HyperLogLog(HLLsize);
    }

    /**
     *
     * @return the header of the column
     */
    public String getLabel() {
        return Label;
    }
    /**
     *
     * @return the hyperloglog representation of the column
     */
    public HyperLogLog getValues() {
        return Values;
    }
//--------------------------------------------------------------------------
    /**
     * add a value to the hyperloglog
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
