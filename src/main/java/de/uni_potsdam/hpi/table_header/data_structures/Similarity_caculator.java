package de.uni_potsdam.hpi.table_header.data_structures;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import de.uni_potsdam.hpi.table_header.data_structures.hyper_table.HTable;
import de.uni_potsdam.hpi.table_header.data_structures.wiki_table.WTable;
import de.uni_potsdam.hpi.table_header.io.Config;
import de.uni_potsdam.hpi.table_header.io.ResultWriter;
import de.uni_potsdam.hpi.table_header.io.Serializer;
import org.apache.commons.lang.StringUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class Similarity_caculator {


    //cashing
    ArrayList<HTable> HLLWEBTABLES = new ArrayList<>();


    //filters??


    //statistics
    int NUMBER_OF_TABLES=0;
    int NUM_NULL_IN_HEADER = 0;
    int NUM_NULL_HEADERS = 0;
    Map<Integer, Integer> TABLES_HEADERS = new HashMap<>();// distribution of number of header rows per table
    Map<Integer, Integer> TABLES_LENGTH = new HashMap<>();// distribution of number of rows per table
    Map<Integer, Integer> TABLES_WIDTH = new HashMap<>();// distribution of number of columns (attributes) per table
    Map<Integer, Integer> TABLES_NUMERIC_COLUMNS = new HashMap<>();// distribution of number of numeric columns (attributes) per table
    Map<Integer, Integer> TABLES_NON_NUMERIC_COLUMNS = new HashMap<>();// distribution of number of non-numeric columns (attributes) per table


    public void initialize() {
        // check if the HLLwebtables is already stored to load it and if not re-create it

        try {
            HLLWEBTABLES = (ArrayList<HTable>) Serializer.deserialize(Config.HYPERTABLE_FILENAME);
        } catch (FileNotFoundException e) {
            //parsing wiki tables file
            parse_wiki_tables();

            //parsing the WDC files
            //TODO 2-add this part

            // store the HLLwebtables
            store_HTables();
            System.out.print("***done building htables***");

        } catch (IOException e) {
            System.err.println("Could not de-serialize the Hypertables");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            System.err.println("Could not cast the de-serialized tables arraylist");
            e.printStackTrace();
        }
    }

    public void caculate_similarity(HTable inputfile, double threasould) {

        for (HTable table: HLLWEBTABLES){

            table.findColumnOverlap(inputfile, threasould);
        }
    }

    private void parse_wiki_tables() {

        GsonBuilder builder = new GsonBuilder();
        builder.setPrettyPrinting().serializeNulls();
        Gson gson = builder.create();
        Stream<String> stream;
        try {

            stream = Files.lines(Paths.get(Config.INPUT_WIKI_FILENAME)).onClose(() -> System.out.println("***Finished reading the file***"));
            //Each line is a json table
            stream.forEach(line -> {
                //binding the json tale to WTable
                WTable web_table = gson.fromJson(line, WTable.class);

                //Update statistics
                update_statistics(web_table, line);

                //add the table to hyper representation
                String table_name="";
                if(web_table.getTableCaption()==null)
                    table_name=web_table.getSectionTitle();
                else
                    table_name=web_table.getTableCaption();
                HTable hyper_table = new HTable(table_name, web_table.getHeaders(), Config.HLLsize);

                for (int i = 0; i < hyper_table.getNumberCols(); i++) {
                    Set<Object> column_value = web_table.getColumnValues(i);
                    if (column_value.size() == 0) {
                        hyper_table.removeEmptyColumn(i);
                    } else
                        for (Object value : column_value) {
                            hyper_table.add2Column(i, value);
                        }
                }
                HLLWEBTABLES.add(hyper_table);
                //System.out.println(hyper_table.getName()+":"+hyper_table.getHeaders());
            });

        } catch (IOException e) {
            e.printStackTrace();
        }

        //write collected statistics to file
        save_statistics();
    }

    private void store_HTables() {
        try {
            Serializer.serialize(HLLWEBTABLES, Config.HYPERTABLE_FILENAME);
        } catch (IOException e) {
            System.err.println("Could not save the hypertables to the disk");
            e.printStackTrace();
        }

    }

    private void update_statistics(WTable table, String line) {

        NUMBER_OF_TABLES++;

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
        ResultWriter.add2Result(table.toString(), Config.Output.SCHEMATA);

        //7-number of tables with null headers
        //8-number of tables with some nulls in header
        //9-tables with null headers as json write to disk
        //TODO: if you added any null representation update here
        boolean null_seen=false;
        boolean non_null_seen=false;

        for(String Value:table.getHeaders())
        {
            if(StringUtils.isBlank(Value))
           {null_seen=true;}
            else
          {non_null_seen=true;}
        }
        if (null_seen)
            NUM_NULL_IN_HEADER++;
        if(!non_null_seen) {
            NUM_NULL_HEADERS++;
            ResultWriter.add2Result(line+"\r\n",Config.Output.TABLES_MISSING_HEADERS);
        }


    }

    private void update_map(int new_value, Map map) {
        if (map.containsKey(new_value))
            map.put(new_value, (int) map.get(new_value) + 1);
        else
            map.put(new_value, 1);


    }

    private void save_statistics() {
        StringBuilder result = new StringBuilder("");
        result.append("Tables : "+ NUMBER_OF_TABLES+"\r\n");
        result.append("NULL HEADERs: "+ NUM_NULL_HEADERS+"\r\n");
        result.append("NULL in header: " + NUM_NULL_IN_HEADER+"\r\n");


        ResultWriter.add2Result(result.toString() , Config.Output.STATISTIC);
        ResultWriter.add2Result(TABLES_LENGTH, Config.Output.LENGTH);
        ResultWriter.add2Result(TABLES_WIDTH, Config.Output.WIDTH);
        ResultWriter.add2Result(TABLES_NUMERIC_COLUMNS, Config.Output.NUMERIC);
        ResultWriter.add2Result(TABLES_NON_NUMERIC_COLUMNS, Config.Output.NON_NUMERIC);

    }
}
