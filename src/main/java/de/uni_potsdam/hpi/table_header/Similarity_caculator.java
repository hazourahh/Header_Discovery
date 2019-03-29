package de.uni_potsdam.hpi.table_header;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
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
import org.aksw.palmetto.Palmetto;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;

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
    public static void initialize(boolean full_corpus, int sampling_percentage) {
        // check if the HLLwebtables is already stored to load it and if not re-create it


        //if we have the full corpus we build testing dataset and then convert to Hyper representation the rest
        if (full_corpus) {

            //1- parse full web table corpus
            HashSet<String> Tables_Supplier = InputReader.parse_wiki_tables_file(Config.FULL_WIKI_FILENAME).collect(Collectors.toCollection(HashSet::new));

            //2- collect statistics
            //Wiki_Dataset_Statistics stat = calculate_wiki_tables_statistics(Tables_Supplier, Config.FULL_WIKI_FILENAME);
            //System.out.println("***statistics about full datasets***");

            //3- sampling to build test datasets
            //TODO: check all this critiria to check the quality of test set
            HashSet<String> test = Sampler.get_ReservoirSample(Tables_Supplier.stream(), sampling_percentage * Config.number_tables / 100);
            test.forEach(
                    line ->
                    {
                        WTable t = WTable.fromString(line);
                        if (t.getNumHeaderRows() == 1 &&
                                !t.has_missing_header() &&
                                t.getNumericColumns().length < 3 &&
                                //t.getNumCols()<6 &&
                                check_header_quality(t.getHeaders())
                        )
                            ResultWriter.add2Result(line + "\n", Config.Output.TEST_SET, Config.FULL_WIKI_FILENAME);
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
                try {
                    IndexWriter writer = InputReader.open_ACSDB_index(IndexWriterConfig.OpenMode.APPEND);

                    train_tables.forEach(json_table -> {
                        WTable wt = WTable.fromString(json_table);

                        if (!wt.has_missing_header_line()) {
                            HLLWEBTABLES.add(wt.Convert2Hyper());
                            Document doc = new Document();
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
                            }
                        }
                    });
                } catch (IOException exp) {
                    System.err.println("Could not open ASDB statistics");
                    e.printStackTrace();
                    System.exit(1);
                }
                System.out.println("***done converting train dataset to HLL representation***");

                // store the HLLwebtables
                store_HTables();
                System.out.println("***done building and storing htables***");

            } catch (IOException e) {
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

        //TODO: improve the search
        //update the candidate set to end up with top k result
        HLLWEBTABLES.forEach(webtable -> {

            //TODO: try other metrics
            long web_dist, input_dist, union_dist;
            float overlap = 0, weighted_overlap, table_overlap;
            Column web_col, input_col;

            //table similarity (context similarity)
            table_overlap = findTableOverlap(inputtable, webtable);

            if (table_overlap > Config.table_similarity) {
                for (int i = 0; i < webtable.getColumns().size(); i++) {
                    for (int j = 0; j < inputtable.getColumns().size(); j++) {
                        web_col = webtable.getColumns().get(i);
                        input_col = inputtable.getColumns().get(j);
                        web_dist = web_col.cardinality();
                        input_dist = input_col.cardinality();
                        HyperLogLogPlus union = new HyperLogLogPlus(Config.HLLsize);
                        try {
                            union.addAll(web_col.getValues());
                            union.addAll(input_col.getValues());
                            union_dist = union.cardinality();
                            if (web_dist > 0 && input_dist > 0)
                                overlap = (web_dist + input_dist - union_dist) / (float) union_dist;

                        } catch (CardinalityMergeException e) {
                            e.printStackTrace();
                            System.exit(1);
                        }
                        //calculate the overlap and add results
                        weighted_overlap = overlap * table_overlap; //comment this to go back to pure jaccard
                        try {
                            if (weighted_overlap > Config.column_similarity &&
                                    web_col.getLabel() != null &&
                                    input_col.getLabel() != null &&
                                    //this.Name !=NULL &&
                                    //t.getName() !=NULL &&
                                    !StringUtils.isBlank(web_col.getLabel()) &&
                                    !StringUtils.isBlank(input_col.getLabel()) && web_col.getLabel().length() <= 50   //TODO: check if this is the right length

                            ) {
                            /*StringBuilder result = new StringBuilder();
                            result.append(webtable.getName().replace(",", "-"));
                            result.append(",");
                            String lhslabel = web_col.getLabel().replace(",", "-");
                            result.append(lhslabel);
                            result.append(",");
                            result.append(inputtable.getName().replace(",", "-"));
                            result.append(",");
                            String rhslabel = input_col.getLabel().replace(",", "-");
                            result.append(rhslabel);
                            result.append(",");
                            result.append(overlap);
                            result.append(",");
                            result.append(weighted_overlap);
                            result.append("\r\n");*/
                                candidates.add_candidate(j, new Header_Candidate(web_col.getLabel(), weighted_overlap));
                                // System.out.println(".");
                                //ResultWriter.add2Result(result.toString(), Config.Output.RESULT,"");
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
     * @param input    table with missing header
     * @param webtable table from the web
     * @return Jaccard similarity between the tables based on Inclusion-exclusion principle   [context similarity]
     */
    private static float findTableOverlap(HTable input, HTable webtable) {
        //TODO: try different metrics
        long LHS_dist, RHS_dist, union_dist;
        HyperLogLogPlus LHS_union, RHS_union, union;

        float overlap = 0;

        LHS_union = new HyperLogLogPlus(Config.HLLsize);
        RHS_union = new HyperLogLogPlus(Config.HLLsize);
        union = new HyperLogLogPlus(Config.HLLsize);

        try {

            for (Column col : webtable.getColumns())
                LHS_union.addAll(col.getValues());
            //cardinality of bag of words of a webtable
            LHS_dist = LHS_union.cardinality();

            for (Column col : input.getColumns())
                RHS_union.addAll(col.getValues());
            //cardinality of bag of words of a inputtable
            RHS_dist = RHS_union.cardinality();


            union.addAll(LHS_union);
            union.addAll(RHS_union);
            //cardinality of bag of words of the union
            union_dist = union.cardinality();

            //intersection cardinality according to Inclusion-exclusion principle
            if (LHS_dist > 0 && RHS_dist > 0)
                overlap = (LHS_dist + RHS_dist - union_dist) / (float) union_dist;

        } catch (CardinalityMergeException e) {
            e.printStackTrace();
            System.exit(1);
        }


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

    private static boolean check_header_quality(List<String> headers) {  //not empty
        if (headers.size() == 0) return false;
        // no empty header longer than 25 or only number
        for (int i = 0; i < headers.size(); i++) {
            if (headers.get(i).isEmpty() || headers.get(i).equals(" ") || headers.get(i).length() > 25 || headers.get(i).matches("[0-9]+") || headers.get(i).contains(")") || headers.get(i).contains("("))
                return false;
        }
        // all unique
        return headers.stream().allMatch(new HashSet<>()::add);
    }
}
