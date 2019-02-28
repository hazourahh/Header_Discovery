package de.uni_potsdam.hpi.table_header.io;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import de.uni_potsdam.hpi.table_header.data_structures.hyper_table.HTable;
import de.uni_potsdam.hpi.table_header.data_structures.statistics_db.ACSDb;
import de.uni_potsdam.hpi.table_header.data_structures.wiki_table.WTable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InputReader {
    //read csv file
    private static List<List<String>> read_CSV_File(String Filename, String seperator) {
        List<List<String>> values = new ArrayList<>();
        Pattern pattern = Pattern.compile(seperator);
        try (Stream<String> lines = Files.lines(Paths.get(Filename))) {
            values = lines.map(line -> Arrays.asList(pattern.split(line))).collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return values;
    }

    //----------------------
    //read a input file as a hypertable
    public static HTable read_WT_File(String Filename, String seperator, boolean has_header) {
        List<List<String>> values = read_CSV_File(Filename, seperator);
        int numcols = values.get(0).size();

        List<String> headers = new ArrayList<>();
        if (!has_header) {
            for (int j = 0; j < numcols; j++)
                headers.add("Column_" + j);
        } else {
            values.get(0).forEach(e -> headers.add(e));
        }

        HTable hyper_table = new HTable("-1", Filename, headers, Config.HLLsize);

        int j = 0;
        if (has_header) j = j + 1;
        do {
            for (int i = 0; i < numcols; i++) {
                hyper_table.add2Column(i, values.get(j).get(i));
            }
            j++;
        } while (j < values.size());

        return hyper_table;
    }
    public static Stream<HTable> parse_wiki_tables_test() {
        Stream.Builder<HTable> builder = Stream.builder();
        Stream<WTable> test_tables = InputReader.parse_wiki_tables_object(Config.TESTING_WIKI_FILENAME);
        test_tables.forEach(wtable -> builder.add(wtable.Convert2Hyper()));
        return builder.build();
    }

    //----------------------------------------------------------------
    //parse ACSDB file into ACSDB representation
    public static ACSDb read_ACSDB_File(String Filename) {
        ACSDb db = new ACSDb();
        //TODO: filter to reduce size
        try (Stream<String> lines = Files.lines(Paths.get(Filename))) {
            lines.forEach(line -> {
                        int first_dash = line.indexOf('_');
                        int last_equal = line.lastIndexOf('=');
                        String type = line.substring(0, first_dash);
                        String the_schema = line.substring(first_dash + 1, last_equal - 1);
                        String freq = line.substring(last_equal + 1).trim();
                        //TODO: check if we need the single attributes
                        if (type.equals("combo") && !the_schema.equals("") && !the_schema.equals(" ")) {
                            db.addSchema(the_schema
                                    , Integer.parseInt(freq));
                        }
                    }

            );
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        return db;
    }

    //----------------------------------------------------------------
    public static Stream<WTable> parse_wiki_tables_object(String file) {
        GsonBuilder builder = new GsonBuilder();
        builder.setPrettyPrinting().serializeNulls();
        Gson gson = builder.create();
        Stream<String> stream;
        Stream<WTable> wiki_objects_stream = null;

        try {

            stream = Files.lines(Paths.get(file)).onClose(() -> System.out.println("***Finished reading the wiki tables file ( " + file + " )"));

            //Each line is a json table
            wiki_objects_stream = stream.map(line ->
                            //1-binding the json table to WTable
                            gson.fromJson(line, WTable.class)
                    // web_table.saveAsCSV(false);
            );

        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        //return only distinct tables according to their Ids
        return wiki_objects_stream.distinct();
    }
    public static Stream<String> parse_wiki_tables_file(String file) {
        Stream<String> stream = null;
        try {

            stream = Files.lines(Paths.get(file)).onClose(() -> System.out.println("***Finished reading the wiki tables file ( " + file + " )"));


        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        //return only distinct tables according to their Ids
        return stream.distinct();
    }


}
