package de.uni_potsdam.hpi.table_header.data_structures.hyper_table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Hazar.Harmouch
 * This class is an in-memory representaion of webtable
 */
public class HTable implements Serializable {

    private String _id;
    private String Name;/* table caption or file name in case of csv input file */
    private ArrayList<Column> Columns = new ArrayList<>();/* Columns (Label and HLL) */
    public static int HLLbitSize;/* size of columns hyperloglog representaion */

    public String get_id() {
        return _id;
    }

    /**
     * Constructor
     * @param name table caption
     * @param HLLsize Hyperloglog size
     */
    public HTable(String id,String name, int HLLsize) {
        super();
        Name = name;
        _id=id;
        HLLbitSize = HLLsize;
    }

    /**
     * Constructor
     * @param name table caption
     * @param columns_labels Arraylist of Columns
     * @param HLLsize Hyperloglog size
     */
    public HTable(String id,String name, List<String> columns_labels, int HLLsize) {
        super();
        Name = name;
        _id=id;
        HLLbitSize = HLLsize;
        columns_labels.forEach(column->Columns.add(new Column(column, HLLsize)));
    }

    /**
     *
     * @return Table Caption
     */
    public String getName() {
        return Name;
    }

    public ArrayList<Column> getColumns() {
        return Columns;
    }

    //-------------------------------------------------------------------------------------------------
    /**
     *
     * @param column_index: the column to be updated
     * @param value: the new value to be added
     */
    public void add2Column(int column_index, Object value) {
        Columns.get(column_index).addValue(value);
    }

    /**
     *
     * @return the number of columns
     */
    public int getNumberCols() {return Columns.size();}

    /**
     *
     * @return arraylist of Columns
     */
    public  ArrayList<String> getHeaders() {
        ArrayList<String> header=new ArrayList<>();
        for (Column c: Columns)
        {header.add(c.getLabel());}
        return header;
    }

    /**
     * remove a column of the specified index
     * @param column_index the index of the column to be removed
     */
    public void removeColumn(int column_index) {Columns.remove(column_index);}


    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!HTable.class.isAssignableFrom(obj.getClass())) {
            return false;
        }

        final HTable other = (HTable) obj;
        if ((this.Name== null) ? (other.Name != null) : !this.Name.equals(other.Name)) {
            return false;
        }

        if (!this.Name.equals(other.Name)) {
            return false;
        }

        if (this.Columns.size()!=other.Columns.size()) {
            return false;
        }
        return true;
    }

}
