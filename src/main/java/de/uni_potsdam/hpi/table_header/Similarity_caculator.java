package de.uni_potsdam.hpi.table_header;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Header_Candidate;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Topk_candidates;
import de.uni_potsdam.hpi.table_header.data_structures.hyper_table.Column;
import de.uni_potsdam.hpi.table_header.data_structures.hyper_table.HTable;
import de.uni_potsdam.hpi.table_header.data_structures.wiki_table.WTable;
import de.uni_potsdam.hpi.table_header.data_structures.wiki_table.Wiki_Dataset_Statistics;
import de.uni_potsdam.hpi.table_header.io.Config;
import de.uni_potsdam.hpi.table_header.io.ResultWriter;
import de.uni_potsdam.hpi.table_header.io.Serializer;
import org.apache.commons.lang.StringUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Stream;

class Similarity_caculator {


    //cashing
    private ArrayList<HTable> HLLWEBTABLES = new ArrayList<>(); //web tables in hyperloglog form
    private  Wiki_Dataset_Statistics statistic=new Wiki_Dataset_Statistics();
    private Topk_candidates candidates;



    Topk_candidates getScoredCandidates() {
        return candidates;
    }


    /**
     * initialize the Hypertables either by parsing json file or deserialize from previous run
     */
    void initialize() {
        // check if the HLLwebtables is already stored to load it and if not re-create it
        try {
            HLLWEBTABLES = (ArrayList<HTable>) Serializer.deserialize(Config.HYPERTABLE_FILENAME);
            System.out.println("***done deserialize htables***");


        } catch (FileNotFoundException e) {
            //parsing wiki tables file
            parse_wiki_tables();

            //parsing the WDC files
            //TODO -add this part

            // store the HLLwebtables
            store_HTables();
            System.out.println("***done building htables***");

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

    /**
     * find the top k candidate for each column in the input table
     * @param inputfile impute table represented as hyper table with dumy headers
     * @param k top k candidate to keep
     */

    void calculate_similarity(HTable inputfile, int k) {
        //topk for each column
        candidates= new Topk_candidates(k,inputfile.getNumberCols());


        //update the candidate set to end up with top k result
        HLLWEBTABLES.forEach(table-> findColumnOverlap(inputfile,table));

        //keep only unique candidates with max score
        //TODO: check if it is a good idea to aggregate the similarity scores

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
                //1-binding the json table to WTable
                WTable web_table = gson.fromJson(line, WTable.class);

               // web_table.saveAsCSV(false);

                //2-Update statistics
               statistic.update_statistics(web_table, line);

                //3-either add it to test tables or add the table to hyper representation
                //TODO add sampling of tables for test later and dont add then to the similarity check

                 HTable hyper_table = new HTable(web_table.getTableName(), web_table.getHeaders(), Config.HLLsize);
                for (int i = 0; i < hyper_table.getNumberCols(); i++) {
                    Set column_value = web_table.getColumnValues(i);
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
            System.exit(1);
        }

        //4-write collected statistics to file
        statistic.save_statistics();
    }

    private void store_HTables() {
        try {
            Serializer.serialize(HLLWEBTABLES, Config.HYPERTABLE_FILENAME);
        } catch (IOException e) {
            System.err.println("Could not save the hypertables to the disk");
            e.printStackTrace();
            System.exit(1);
        }

    }

    /**
     *
     * @param inputtable  table with missing header
     * @param webtable table from the web
     * updathe the top k candidate according to the weighted-Jaccard similarity that combines [content similarity] jaccard between two columns weighted by table overlap
     */
    private void findColumnOverlap(HTable inputtable, HTable webtable) {

        //TODO: try other metrics
        long web_dist, input_dist, union_dist;
        float overlap = 0, weighted_overlap, table_overlap;
        Column web_col, input_col;

        //table similarity (context similarity)
        table_overlap = findTableOverlap(inputtable,webtable);

if( table_overlap>0.3) {
    for (int i = 0; i < webtable.getColumns().size(); i++) {
        for (int j = 0; j < inputtable.getColumns().size(); j++) {
            web_col = webtable.getColumns().get(i);
            input_col = inputtable.getColumns().get(j);
            web_dist = web_col.cardinality();
            input_dist = input_col.cardinality();
            HyperLogLog union = new HyperLogLog(HTable.HLLbitSize);
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
            //caculate the overlap and add results
            weighted_overlap = overlap * table_overlap; //comment this to go back to pure jaccard
            try {
                if (weighted_overlap > 0.5 &&
                        web_col.getLabel() != null &&
                        input_col.getLabel() != null &&
                        //this.Name !=NULL &&
                        //t.getName() !=NULL &&
                        !StringUtils.isBlank(web_col.getLabel()) &&
                        !StringUtils.isBlank(input_col.getLabel())) {
                    StringBuilder result = new StringBuilder();
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
                    result.append("\r\n");
                    candidates.add_candidate(j, new Header_Candidate(lhslabel, weighted_overlap));
                    System.out.println(".");
                    ResultWriter.add2Result(result.toString(), Config.Output.RESULT);
                }

            } catch (Exception e) {
                System.out.println("error in find table" + webtable.getName() + "-" + inputtable.getName());
                System.exit(1);
            }
        }


    }
}
    }

    /**
     *
     * @param input  table with missing header
     * @param webtable table from the web
     * @return  Jaccard similarity between the tables based on Inclusion-exclusion principle   [context similarity]
     */
    private float findTableOverlap(HTable input, HTable webtable) {
        //TODO: try different metrics
        long LHS_dist, RHS_dist, union_dist;
        HyperLogLog LHS_union, RHS_union,union;

        float overlap = 0;

        LHS_union=new HyperLogLog(HTable.HLLbitSize);
        RHS_union=new HyperLogLog(HTable.HLLbitSize);
        union=new HyperLogLog(HTable.HLLbitSize);

        try {

            for (Column col:webtable.getColumns())
                LHS_union.addAll(col.getValues());
            //cardinality of bag of words of a webtable
            LHS_dist=LHS_union.cardinality();

            for (Column col:input.getColumns())
                RHS_union.addAll(col.getValues());
            //cardinality of bag of words of a inputtable
            RHS_dist=RHS_union.cardinality();


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

}
