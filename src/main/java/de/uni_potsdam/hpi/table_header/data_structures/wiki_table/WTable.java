package de.uni_potsdam.hpi.table_header.data_structures.wiki_table;

import de.uni_potsdam.hpi.table_header.io.Config;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
    private List<List<Cell>> tableData = new ArrayList<>();
    private List<List<Cell>> tableHeaders = new ArrayList<>();
    private int tableId;


    /**
     * setter and getter
     */
    public String get_id() {
        return _id;
    }

    int getNumCols() {
        return numCols;
    }

    int getNumDataRows() {
        return numDataRows;
    }

    public int getNumHeaderRows() {
        return numHeaderRows;
    }

    int[] getNumericColumns() {
        return numericColumns;
    }

    public long getPgId() {
        return pgId;
    }

    public String getPgTitle() {
        return pgTitle;
    }

    private String getSectionTitle() {
        return sectionTitle;
    }

    private String getTableCaption() {
        return tableCaption;
    }

    public List<List<Cell>> getTableData() {
        return tableData;
    }

    public List<List<Cell>> getTableHeaders() {
        return tableHeaders;
    }

    public int getTableId() { return tableId; }

    public String getTableName() {
        String table_name;
        if (getTableCaption() == null)
            table_name = getSectionTitle();
        else
            table_name = getTableCaption();
        return table_name;
    }

    public void saveAsCSV(boolean withheader) {

        File directory = new File(Config.TABLEASCSV_Folder);

        if (!directory.exists())
           directory.mkdir();

            File file = new File(Config.TABLEASCSV_Folder + getTableName() + ".csv");
            if (!file.exists()) {
                try {
                    boolean created = file.createNewFile();
                    if (created) {
                        BufferedWriter bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile(), true));
                        bw.write(getTableAsCSV(withheader));
                        bw.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

    }

    /**
     * @param column_id index of the column we neeed to know its label
     * @return header of the column
     */
    private String getColumnHeader(int column_id) {
        StringBuilder label = new StringBuilder();
        //concatenate the labels form all levels
        for (int i = 0; i < numHeaderRows; i++)
            label.append(tableHeaders.get(i).get(column_id).getText()).append(" ");
        return label.toString();
    }

    /**
     * @return the header line of the table
     */
    public List<String> getHeaders() {
        ArrayList<String> headers = new ArrayList<>();
        for (int i = 0; i < numCols; i++)
            headers.add(getColumnHeader(i));
        return headers;
    }

    /**
     * @param col index of the column
     * @return the values of this column except empty and numeric values
     */
    public Set getColumnValues(int col) {
        // TODO: note that we eliminate any repetition here!
        Set<String> col_vales = new HashSet<>();
        //check if it has a header and how many header lines: ignore tables without headers and with more than one label
        if (//numHeaderRows >= 0 &&
                col > -1 && col < numCols) {
            for (int i = 0; i < numDataRows; i++) {
                String value = tableData.get(i).get(col).getText();
                // TODO: we can filter other types of null in here
                // "" and " " filtered
                // no numeric columns
                if (value != null && !value.equals("") && !value.equals(" "))// && !tableData.get(i).get(col).isNumeric())
                    col_vales.add(value);
            }
        }
        return col_vales;
    }


    private String getTableAsCSV(boolean withheader) {
     String thefile="";
     if (withheader)
         thefile=String.join(",",getHeaders())+"\n";
      thefile+= tableData.stream()
                  .map(row-> String.join(",",row.stream().map(Cell::getText).collect(Collectors.toList())))
        .collect(Collectors.joining("\n"));


        return thefile;

    }

    @Override
    public String toString() {
        return pgTitle +
                "," +
                tableCaption +
                "," +
                getHeaders() +
                "\r\n";
    }
}

