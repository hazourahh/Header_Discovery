package de.uni_potsdam.hpi.table_header.io;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import de.uni_potsdam.hpi.table_header.data_structures.hyper_table.HTable;
import de.uni_potsdam.hpi.table_header.data_structures.wiki_table.WTable;
import org.aksw.palmetto.Palmetto;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
/**
 * @author Hazar Harmouch
 *
 */
public final class InputReader {
    //read csv file
    public static List<List<String>> read_CSV_File(String Filename, String seperator) {
        List<List<String>> values = new ArrayList<>();
        Pattern pattern = Pattern.compile(seperator);
        try (Stream<String> lines = Files.lines(Paths.get(Filename))) {
            values = lines.map(line -> Arrays.asList(pattern.split(line))).collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return values;
    }


    //-------------------------------Pars input ----------------------------------
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

    //-------------------------------ACSDB----------------------------------------
    /**
     *  parse ACSDB file and build a lucene index of it
     * @param Filename ACSDB file
     */
    public static void build_ACSDB_index(String Filename) {

        try {
            IndexWriter writer = open_ACSDB_index(IndexWriterConfig.OpenMode.CREATE);

            //pasrs ACSDB and add schemata to the index
            //TODO: filter to reduce size
            Stream<String> lines = Files.lines(Paths.get(Filename));
            lines.forEach(line -> {
                        //extract the schema and the frequency
                        int first_dash = line.indexOf('_');
                        int last_equal = line.lastIndexOf('=');
                        String type = line.substring(0, first_dash);
                        String the_schema = line.substring(first_dash + 1, last_equal - 1);
                        String freq = line.substring(last_equal + 1).trim();
                        //TODO: check if we need the single attributes

                        if (type.equals("combo") && !the_schema.equals("") && !the_schema.equals(" ")) {

                            String processed_schema = Arrays.stream(the_schema.split("_"))
                                    .map(e -> e.replaceAll("[\\]\\[(){},.;:!?<>%\\-*]", " "))
                                    .map(String::trim)
                                    .map(String::toLowerCase)
                                    .map(e -> e.replaceAll(" ", "_"))
                                    .collect(Collectors.joining(" "));
                            Document doc = new Document();
                            doc.add(new TextField(Palmetto.DEFAULT_TEXT_INDEX_FIELD_NAME, processed_schema, TextField.Store.YES));
                            try {
                                //add the document n time where n is the frequency of this schema
                                    for (int i = 0; i < Integer.parseInt(freq); i++)
                                        writer.addDocument(doc);
                            } catch (IOException e) {
                                e.printStackTrace();
                                System.exit(1);
                            }

                        }
                    }

            );

            close_ACSDB_index(writer);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static IndexWriter open_ACSDB_index(IndexWriterConfig.OpenMode mode) throws IOException
    {
        //index directory
        File indexpath = new File(Config.index_Folder);
        indexpath.mkdirs();
        Directory dir = FSDirectory.open(indexpath);
        Analyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_44);
        IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_44, analyzer);

        //create a new index in the directory and remove any previous indexed docs
        iwc.setOpenMode(mode);
        return new IndexWriter(dir, iwc);

    }

    public static void close_ACSDB_index(IndexWriter writer) throws IOException{
            writer.forceMerge(1);
            writer.close();

    }


    //-------------------------------WIKI Web Tables---------------------------------

    /**
     *
     * @param file  path to wiki table json file
     * @return a stream of web tables objects
     */
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

    /**
     *
     * @param file path to wiki table json file
     * @return a stream of strings of json objects
     */
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
