package de.uni_potsdam.hpi.table_header.data_structures.wiki_table;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import de.uni_potsdam.hpi.table_header.data_structures.hyper_table.HTable;
import de.uni_potsdam.hpi.table_header.io.Config;
import org.apache.commons.lang.StringUtils;

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

    public int getNumCols() {
        return numCols;
    }

    public int getNumDataRows() {
        return numDataRows;
    }

    public int getNumHeaderRows() {
        return numHeaderRows;
    }

    public int[] getNumericColumns() {
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

    public int getTableId() {
        return tableId;
    }
//--------------------------------------------------------------------------

    /***
     *
     * @return table caption as a name or section title if no caption
     */
    public String getTableName() {
        String table_name;
        if (getTableCaption() == null)
            table_name = getSectionTitle();
        else
            table_name = getTableCaption();
        return table_name;
    }

    /***
     *
     * @param withheader
     * @return
     */
    public String getTableAsCSV(boolean withheader) {
        String thefile = "";
        if (withheader)
            thefile = String.join(",", getHeaders()) + "\n";
        thefile += tableData.stream()
                .map(row -> String.join(",", row.stream().map(Cell::getText).collect(Collectors.toList())))
                .collect(Collectors.joining("\n"));
        return thefile;

    }


    /***
     *
     * @return the Hyper table representation of the web table
     */
    public HTable Convert2Hyper() {

        HTable hyper_table = new HTable(get_id(), getTableName(), getHeaders(), Config.HLLsize);
        for (int i = 0; i < hyper_table.getNumberCols(); i++) {
            Set column_value = getColumnValues(i);
            if (column_value.size() == 0) {
                hyper_table.removeColumn(i);
            } else
                for (Object value : column_value) {
                    hyper_table.add2Column(i, value);
                }
        }
        return hyper_table;
    }

    /**
     * @param column_id index of the column we neeed to know its label
     * @return header of the column
     */
    public String getColumnHeader(int column_id) {
        StringBuilder label = new StringBuilder();
        //TODO: concatenate the labels form all levels?
        //for (int i = 0; i < numHeaderRows; i++)
        label.append(tableHeaders.get(numHeaderRows - 1).get(column_id).getText());//.append(" ");
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
     * @return the distinct non empty values of this column
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
                if (value != null && !value.equals("") && !value.equals(" "))// && !tableData.get(i).get(col).isNumeric())
                    col_vales.add(value);
            }
        }
        return col_vales;
    }

    /***
     *
     * @return return string JSON representation of this table
     */
    public String convert2JSON() {
        GsonBuilder builder = new GsonBuilder();
        builder.serializeNulls();
        Gson gson = builder.create();
        return gson.toJson(this);
    }

    /***
     *
     * @return true if one at least of the labels is missing
     */
    public boolean has_missing_header() {
        //TODO: if you added any null representation update here
        boolean null_seen = false;
        List<String> headers = getHeaders();
        if (headers == null || headers.isEmpty())
            null_seen = true;
        else
            for (String Value : headers) if (StringUtils.isBlank(Value)) null_seen = true;

        return null_seen;
    }

    /***
     *
     * @return true if the table has a missing header line
     */
    public boolean has_missing_header_line() {
        //TODO: if you added any null representation update here
        boolean non_null_seen = false;

        for (String Value : getHeaders()) {
            if (!StringUtils.isBlank(Value)) {
                non_null_seen = true;
            }
        }
        return !non_null_seen;
    }

    public static WTable fromString(String json_string) {
        GsonBuilder builder = new GsonBuilder();
        builder.setPrettyPrinting().serializeNulls();
        Gson gson = builder.create();
        return gson.fromJson(json_string, WTable.class);
    }

    /***
     *   write the current table in csv representation
     * @param withheader rite header line or not
     */
    public static void save_WTable_As_CSV(WTable wt, boolean withheader) {

        File directory = new File(Config.TABLEASCSV_Folder);

        if (!directory.exists())
            directory.mkdir();

        File file = new File(Config.TABLEASCSV_Folder + wt.getTableName() + ".csv");
        if (!file.exists()) {
            try {
                boolean created = file.createNewFile();
                if (created) {
                    BufferedWriter bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile(), true));
                    bw.write(wt.getTableAsCSV(withheader));
                    bw.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

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

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!WTable.class.isAssignableFrom(obj.getClass())) {
            return false;
        }

        final WTable other = (WTable) obj;
        if ((this._id == null) ? (other._id != null) : !this._id.equals(other._id)) {
            return false;
        }

        if (!this._id.equals(other._id)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return _id.hashCode();
    }

}

