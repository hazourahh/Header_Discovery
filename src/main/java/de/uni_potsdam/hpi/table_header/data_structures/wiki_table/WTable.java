package de.uni_potsdam.hpi.table_header.data_structures.wiki_table;

import java.io.Serializable;
import java.util.*;

/**
 * @author Hazar.Harmouch
 * This class is a representaion of wikitables from http://websail-fe.cs.northwestern.edu/TabEL/
 */

public class WTable implements Serializable {

    private static final long serialVersionUID = 1L;

    private String _id;
    private int numCols;
    private int numDataRows;
    private int numHeaderRows;
    private int[] numericColumns;
    private double order;
    private long pgId;
    private String pgTitle;
    private String sectionTitle;
    private String tableCaption;
    private List<List<Cell>> tableData=new ArrayList<>();
    private List<List<Cell>> tableHeaders=new ArrayList<>();
    private int tableId;

    /**
     *
     * @param column_id index of the column we neeed to know its label
     * @return header of the column
     */
    public String getColumnHeader(int column_id)
    {    String label="";
        //concatenate the labels form all levels
        for(int i=0;i<numHeaderRows;i++)
            label+=tableHeaders.get(i).get(column_id).getText()+" ";

       return label;
    }

    /**
     *
     * @return the header line of the table
     */
    public List<String> getHeaders()
    { ArrayList<String> headers=new ArrayList<>();
        for(int i=0;i<numCols;i++)
            headers.add(getColumnHeader(i));
       return headers;
    }

    /**
     *
     * @param col index of the column
     * @return the values of this column except empty and numeric values
     */
    public Set getColumnValues(int col) {
        // TODO: note that we elementaed any repetatiopn here!
        Set col_vales = new HashSet() ;
        //check if it has a header and how many header lines: ignore tables without headers and with more than one label
        if (//numHeaderRows >= 0 &&
             col > -1 && col < numCols) {
            for (int i = 0; i < numDataRows; i++) {
                String value=tableData.get(i).get(col).getText();
                // TODO: we can filter other types of null in here
                // "" and " " filtered
                // no numeric columns
                if (value!=null && !value.equals("") && !value.equals(" ") && !tableData.get(i).get(col).isNumeric())
                    col_vales.add(value);
            }
        }
        return col_vales;
    }

    /**
     * setter and getter
     */
    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public int getNumCols() {
        return numCols;
    }

    public void setNumCols(int numCols) {
        this.numCols = numCols;
    }

    public int getNumDataRows() {
        return numDataRows;
    }

    public void setNumDataRows(int numDataRows) {
        this.numDataRows = numDataRows;
    }

    public int getNumHeaderRows() {
        return numHeaderRows;
    }

    public void setNumHeaderRows(int numHeaderRows) {
        this.numHeaderRows = numHeaderRows;
    }

    public int[] getNumericColumns() {
        return numericColumns;
    }

    public void setNumericColumns(int[] numericColumns) {
        this.numericColumns = numericColumns;
    }

    public double getOrder() {
        return order;
    }

    public void setOrder(double order) {
        this.order = order;
    }

    public long getPgId() {
        return pgId;
    }

    public void setPgId(long pgId) {
        this.pgId = pgId;
    }

    public String getPgTitle() {
        return pgTitle;
    }

    public void setPgTitle(String pgTitle) {
        this.pgTitle = pgTitle;
    }

    public String getSectionTitle() {
        return sectionTitle;
    }

    public void setSectionTitle(String sectionTitle) {
        this.sectionTitle = sectionTitle;
    }

    public String getTableCaption() {
        return tableCaption;
    }

    public void setTableCaption(String tableCaption) {
        this.tableCaption = tableCaption;
    }

    public List<List<Cell>> getTableData() {
        return tableData;
    }

    public void setTableData(List<List<Cell>> tableData) {
        this.tableData = tableData;
    }

    public List<List<Cell>> getTableHeaders() {
        return tableHeaders;
    }

    public void setTableHeaders(List<List<Cell>> tableHeaders) {
        this.tableHeaders = tableHeaders;
    }

    public int getTableId() {
        return tableId;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append(pgTitle);
        result.append(",");
        result.append(tableCaption);
        result.append(",");
        result.append(getHeaders());
        result.append("\r\n");
        return result.toString();
    }
}

