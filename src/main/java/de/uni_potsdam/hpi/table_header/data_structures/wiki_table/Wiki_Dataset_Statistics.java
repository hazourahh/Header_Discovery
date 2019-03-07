package de.uni_potsdam.hpi.table_header.data_structures.wiki_table;

import de.uni_potsdam.hpi.table_header.io.Config;
import de.uni_potsdam.hpi.table_header.io.ResultWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
/**
 * @author Hazar Harmouch
 *
 */
public class Wiki_Dataset_Statistics {

    //statistics
    private static int NUMBER_OF_TABLES=0;
    private static int NUM_NULL_IN_HEADER = 0;
    private static int NUM_NULL_HEADERS = 0;
    private static  Map<Integer, Integer> TABLES_HEADERS = new HashMap<>();// distribution of number of header rows per table
    private static Map<Integer, Integer> TABLES_LENGTH = new HashMap<>();// distribution of number of rows per table
    private static  Map<Integer, Integer> TABLES_WIDTH = new HashMap<>();// distribution of number of columns (attributes) per table
    private static Map<Integer, Integer> TABLES_NUMERIC_COLUMNS = new HashMap<>();// distribution of number of numeric columns (attributes) per table
    private static Map<Integer, Integer> TABLES_NON_NUMERIC_COLUMNS = new HashMap<>();// distribution of number of non-numeric columns (attributes) per table
    private static String FILE_NAME;


    public void update_statistics(WTable table, String file_name) {
        FILE_NAME=file_name;
        NUMBER_OF_TABLES++;
        boolean nullable=false;
        int numeric_col = table.getNumericColumns().length;
        int col = table.getNumCols();

        //1-table length
        update_map(table.getNumDataRows(), TABLES_LENGTH);

        //2-table width
        update_map(col, TABLES_WIDTH);

        //3-number of numeric columns
        update_map(numeric_col, TABLES_NUMERIC_COLUMNS);

        //4-number of non-mumeric columns
        update_map(col - numeric_col, TABLES_NON_NUMERIC_COLUMNS);

        //5-both numeric non-numeric with table name
        //ResultWriter.add2Result();

        //6-schema with table name
        ResultWriter.add2Result(table.toString(), Config.Output.SCHEMATA,FILE_NAME);

        //7-number of tables with null headers
        //8-number of tables with some nulls in header
        //9-tables with null headers as json write to disk
        if (table.has_missing_header())
        {NUM_NULL_IN_HEADER++;
        }
        if(table.has_missing_header_line()) {
            NUM_NULL_HEADERS++;
            ResultWriter.add2Result(table.convert2JSON()+"\r\n",Config.Output.TABLES_MISSING_HEADERS,FILE_NAME);
        }
    }

    private void update_map(int new_value, Map<Integer, Integer> map) {
        if (map.containsKey(new_value))
            map.put(new_value, map.get(new_value) + 1);
        else
            map.put(new_value, 1);


    }

    public void save_statistics() {
        String result = "Tables : " + NUMBER_OF_TABLES + "\r\n" +
                "NULL HEADERs: " + NUM_NULL_HEADERS + "\r\n" +
                "NULL in header: " + NUM_NULL_IN_HEADER + "\r\n";
        ResultWriter.add2Result(result, Config.Output.STATISTIC,FILE_NAME);
        ResultWriter.add2Result(TABLES_LENGTH, Config.Output.LENGTH,FILE_NAME);
        ResultWriter.add2Result(TABLES_WIDTH, Config.Output.WIDTH,FILE_NAME);
        ResultWriter.add2Result(TABLES_NUMERIC_COLUMNS, Config.Output.NUMERIC,FILE_NAME);
        ResultWriter.add2Result(TABLES_NON_NUMERIC_COLUMNS, Config.Output.NON_NUMERIC,FILE_NAME);

    }

public int getMax_Length()
{return TABLES_LENGTH.keySet().stream().mapToInt(v -> v).max().orElseThrow(NoSuchElementException::new); }

public int getMax_Width()
{return TABLES_WIDTH.keySet().stream().mapToInt(v -> v).max().orElseThrow(NoSuchElementException::new);}

public int getMax_Num_Numericcols()
{return TABLES_NUMERIC_COLUMNS.keySet().stream().mapToInt(v -> v).max().orElseThrow(NoSuchElementException::new);}

public int get_NumTables()
{return NUMBER_OF_TABLES;}
}
