package de.uni_potsdam.hpi.table_header;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import de.uni_potsdam.hpi.table_header.Util.Sampler;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Header_Candidate;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Topk_candidates;
import de.uni_potsdam.hpi.table_header.data_structures.hyper_table.Column;
import de.uni_potsdam.hpi.table_header.data_structures.hyper_table.HTable;
import de.uni_potsdam.hpi.table_header.data_structures.wiki_table.WTable;
import de.uni_potsdam.hpi.table_header.data_structures.wiki_table.Wiki_Dataset_Statistics;
import de.uni_potsdam.hpi.table_header.io.Config;
import de.uni_potsdam.hpi.table_header.io.InputReader;
import de.uni_potsdam.hpi.table_header.io.ResultWriter;
import de.uni_potsdam.hpi.table_header.io.Serializer;
import org.apache.commons.lang.StringUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * @author Hazar Harmouch
 */
class Similarity_caculator {


    private static ArrayList<HTable> HLLWEBTABLES = new ArrayList<>(); //web tables in hyperloglog form

//------------------------------------------------------------------------------------

    /**
     * initialize the Hypertables either by parsing json file or deserialize from previous run
     */
    //TODO: add capability to update the tables if they are already stored
    public static void initialize(boolean sample, int sampling_percentage) {

        //if we have the full corpus we build testing dataset and then convert to Hyper representation the rest
        if (sample) {

            //1- parse full web table corpus
            HashSet<String> Tables_Supplier = InputReader.parse_wiki_tables_file(Config.FULL_WIKI_FILENAME).collect(Collectors.toCollection(HashSet::new));

           /* Tables_Supplier.forEach(line ->
                    {
                        WTable wt = WTable.fromString(line);
                        if ( wt.has_missing_header_line() ||  wt.getNumDataRows()<3 || wt.getNumCols()<2
                        ) {System.out.println("IGNORE:"+wt.getTableName()+"("+wt.getNumCols()+","+wt.getNumDataRows()+")");}
                        else { System.out.println(wt.getTableName()+"("+wt.getNumCols()+","+wt.getNumDataRows()+")");
                            ResultWriter.add2Result(line + "\n", Config.Output.FILTERED_SET, Config.FULL_WIKI_FILENAME);
                        }
                    }
            );*/
            //2- collect statistics
            //Wiki_Dataset_Statistics stat = calculate_wiki_tables_statistics(Tables_Supplier, Config.FULL_WIKI_FILENAME);
            //System.out.println("***statistics about full datasets***");

            //3- sampling to build test datasets
            //TODO: check all this critiria to check the quality of test set

            HashSet<String> test = Sampler.get_ReservoirSample(Tables_Supplier.stream(), sampling_percentage * Config.number_tables/ 100);
            //HashSet<String> test = Sampler.get_ReservoirSample(Tables_Supplier.stream(), 4000);

            test.forEach(
                    line ->
                    { //TODO: check the quality of a test table
                        //1-has one header row
                        //2-no missing headers
                        //3-rows>3   cols>2   numeric-col<3
                        //4- no empty header or header contains ( or ) or only numbers or  longer than 25 characters
                        WTable t = WTable.fromString(line);
                        if (t.getNumHeaderRows() == 1 &&
                                !t.has_missing_header() &&
                                t.getNumDataRows()>2 &&
                                t.getNumericColumns().length < 3 &&
                                t.getNumCols()<11 &&
                                t.getNumCols()>=2 &&
                                check_header_quality(t.getHeaders())
                        )
                            ResultWriter.add2Result(line + "\n", Config.Output.TEST_SET, Config.FULL_WIKI_FILENAME);
                        else
                            ResultWriter.add2Result(line + "\n", Config.Output.TRAIN_SET, Config.FULL_WIKI_FILENAME);
                    }
            );
            System.out.println("***done sampling and writing test data***");

            //5- write the rest as training data
            Tables_Supplier.forEach(line -> {
                if (!test.contains(line))
                    ResultWriter.add2Result(line + "\n", Config.Output.TRAIN_SET, Config.FULL_WIKI_FILENAME);
            });

            System.out.println("***done writing train dataset ***");
        }

       try {

            HLLWEBTABLES = (ArrayList<HTable>) Serializer.deserialize(Config.HYPERTABLE_FILENAME);
            System.out.println("***done deserialize htables***");

        } catch (FileNotFoundException e) {


            //0- load or build schemata statistics
            Coherent_Blinder.initialize();

            //1- parse training data
            Stream<String> train_tables = InputReader.parse_wiki_tables_file(Config.TRAINING_WIKI_FILENAME);

            // 2-caculate statistics
            // calculate_wiki_tables_statistics(train_tables.get(), Config.FULL_WIKI_FILENAME);

            //3-convert to HLLwebtables and write headers to the index
            // try {
            //  IndexWriter writer = InputReader.open_ACSDB_index(IndexWriterConfig.OpenMode.APPEND);

           long start=System.currentTimeMillis();
            train_tables.forEach(json_table -> {
                WTable wt = WTable.fromString(json_table);
                //filter out low quality tables
                if (!wt.has_missing_header_line())
                    HLLWEBTABLES.add(wt.Convert2Hyper());
                            /*Document doc = new Document();
                            doc.add(new TextField(Palmetto.DEFAULT_TEXT_INDEX_FIELD_NAME,
                                    String.join(" ",
                                            wt.getHeaders().stream().map(ee -> ee.replace(" ", "_")).map(String::trim).map(String::toLowerCase).collect(Collectors.toList()))
                                    , TextField.Store.YES));
                            try {
                                writer.addDocument(doc);
                            } catch (IOException exp) {
                                System.err.println("Could not write to ACSDB statistics");
                                e.printStackTrace();
                                System.exit(1);
                            }*/
            });
            long end=System.currentTimeMillis();
            System.out.println(((end- start) / 1000)+"***done converting train dataset to HLL representation***");

            // store the HLLwebtables
            store_HTables();
            System.out.println("***done building and storing htables***");

        }
        catch (IOException e) {
            System.err.println("Could not de-serialize the Hypertables");
            e.printStackTrace();
            System.exit(1);
        } catch (ClassNotFoundException e) {
            System.err.println("Could not cast the de-serialized tables arraylist");
            e.printStackTrace();
            System.exit(1);
        }

  }

    private static void store_HTables() {
        try {
            Serializer.serialize(HLLWEBTABLES, Config.HYPERTABLE_FILENAME);
        } catch (IOException e) {
            System.err.println("Could not save the hypertables to the disk");
            e.printStackTrace();
            System.exit(1);
        }

    }

    //---------------------------- Similarity search----------------------------------

    /**
     * find the top k candidate for each column in the input table
     *
     * @param inputtable impute table represented as hyper table with dumy headers
     * @param k          top k candidate to keep
     */
    public static Topk_candidates calculate_similarity(HTable inputtable, int k) {

        //topk for each column
        Topk_candidates candidates = new Topk_candidates(k, inputtable.getNumberCols());

        HyperLogLogPlus inputtable_HLL = merge(inputtable);


        //TODO: improve the search
        //update the candidate set to end up with top k result
        HLLWEBTABLES.forEach(webtable -> {

            //TODO: try other metrics
            long web_dist, input_dist, union_dist;
            double overlap = 0, weighted_overlap, table_overlap=0;
            Column web_col, input_col;

            //table similarity (context similarity)
            if(Config.table_similarity_filtering)
                 table_overlap = findTableOverlap(inputtable_HLL, webtable);

            if ((Config.table_similarity_filtering && table_overlap > Config.table_similarity) || (!Config.table_similarity_filtering))
            {
                for (int i = 0; i < webtable.getColumns().size(); i++) {
                    for (int j = 0; j < inputtable.getColumns().size(); j++) {

                        web_col = webtable.getColumns().get(i);
                        input_col = inputtable.getColumns().get(j);
                        web_dist = web_col.cardinality();
                        input_dist = input_col.cardinality();
                        HyperLogLogPlus union = merge_HLL(web_col.getValues(),input_col.getValues());
                        union_dist = union.cardinality();

                            if (input_dist  > 0 && web_dist > 0)
                                overlap = (web_dist + input_dist - union_dist) / (float) input_dist;

                        //calculate the overlap and add results
                        if(Config.table_similarity_filtering && Config.table_similarity_weighting)
                        weighted_overlap = overlap * (Config.table_similarity_weight* table_overlap);
                        else
                        weighted_overlap = overlap;
                        try {
                            if (weighted_overlap > Config.column_similarity &&
                                    web_col.getLabel() != null &&
                                    input_col.getLabel() != null &&
                                    //this.Name !=NULL &&
                                    //t.getName() !=NULL &&
                                    !StringUtils.isBlank(web_col.getLabel()) &&
                                    !StringUtils.isBlank(input_col.getLabel()) &&
                                     //TODO: check if this is the right length
                                     web_col.getLabel().length() <= 25  &&
                                    !web_col.getLabel().matches("[0-9]+")

                            ) {

                                candidates.add_candidate(j, new Header_Candidate(web_col.getLabel(), weighted_overlap));
                            }

                        } catch (Exception e) {
                            System.out.println("error in find table" + webtable.getName() + "-" + inputtable.getName());
                            e.printStackTrace();
                            System.exit(1);
                        }
                    }


                }
            }
        });

        return candidates;
        //TODO: check if it is a good idea to aggregate the similarity scores
    }

    /**
     * @param input_HLL    table with missing header
     * @param webtable table from the web
     * @return Jaccard similarity between the tables based on Inclusion-exclusion principle   [context similarity]
     */
 private static float findTableOverlap(HyperLogLogPlus input_HLL, HTable webtable) {


        float overlap = 0;

        HyperLogLogPlus web_HLL = merge(webtable);
        long input_dist=input_HLL.cardinality();
        long web_dist=web_HLL.cardinality();

        //early pruning
        if (input_dist  <= 0 || web_dist <= 0) return -1;
        if(Math.min(input_dist,web_dist)/ (float) Math.max(input_dist,web_dist)<Config.table_similarity) return -1;


        HyperLogLogPlus union=merge_HLL(input_HLL,web_HLL);
        long union_dist=union.cardinality();
        //intersection cardinality according to Inclusion-exclusion principle

                overlap = (input_dist + web_dist - union_dist) / (float) union_dist ;

        return overlap;

    }

    //---------------------------------------- collect statistics-------------------------

    public Wiki_Dataset_Statistics calculate_wiki_tables_statistics(Stream<WTable> tables, String dataset) {
        Wiki_Dataset_Statistics statistic = new Wiki_Dataset_Statistics();

        //Update statistics
        tables.forEach(web_table -> {
            statistic.update_statistics(web_table, dataset);
        });

        //write collected statistics to file
        statistic.save_statistics();
        return statistic;
    }

    //------------------------------------------------- Sampling---------------------------

    private static boolean check_header_quality(List<String> headers) {
        //not empty
        if (headers.size() == 0) return false;
        // no empty header longer than 25 or only number
        for (int i = 0; i < headers.size(); i++) {
            if (headers.get(i).isEmpty() ||
                    headers.get(i).equals(" ") ||
                    headers.get(i).length() > 25 ||
                    headers.get(i).matches("[0-9]+") ||
                    headers.get(i).contains(")") ||
                    headers.get(i).contains("("))
                return false;
        }
        // all unique
        return headers.stream().allMatch(new HashSet<>()::add);
    }


private static HyperLogLogPlus merge(HTable table)
{
    HyperLogLogPlus  web_union = new HyperLogLogPlus(Config.HLL_PLUS_P,Config.HLL_PLUS_SP);
    try {
        for (Column col : table.getColumns()) {
            web_union.addAll(col.getValues());
        }
    } catch (Exception e) {
        e.printStackTrace();
        System.exit(1);
    }
    return web_union;
}

    private static HyperLogLogPlus merge_HLL(HyperLogLogPlus sk1, HyperLogLogPlus sk2)
    {
        HyperLogLogPlus  union = new HyperLogLogPlus(Config.HLL_PLUS_P,Config.HLL_PLUS_SP);
        try {
            union.addAll(sk1);
            union.addAll(sk2);

        }catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return union;
    }

}
