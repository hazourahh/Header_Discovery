package de.uni_potsdam.hpi.table_header.benchmark;


import de.uni_potsdam.hpi.table_header.data_structures.Result.Candidate;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Header_Candidate;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Topk_candidates;
import de.uni_potsdam.hpi.table_header.data_structures.wiki_table.WTable;
import de.uni_potsdam.hpi.table_header.io.Config;
import de.uni_potsdam.hpi.table_header.io.InputReader;
import de.uni_potsdam.hpi.table_header.io.ResultWriter;
import lazo.index.LazoIndex;
import lazo.sketch.LazoSketch;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Hazar Harmouch
 */

public class LAZO_test {

        public static void main(String[] args) {
            int lazo_k=1;
            LazoIndex index = new LazoIndex(lazo_k);
            {
                long startTime_index = 0, stopTime_index = 0, index_time = 0;
                startTime_index =System.currentTimeMillis();
                System.out.println("***start indexing training dataset***");

                //1- parse training data
                Stream<String> train_tables = InputReader.parse_wiki_tables_file(Config.TRAINING_WIKI_FILENAME);

                train_tables.forEach(json_table -> {
                    WTable wt = WTable.fromString(json_table);

                    //filter out low quality tables
                    if (!wt.has_missing_header_line()) {
                        for (int i = 0; i < wt.getNumCols(); i++) {
                            Set column_value = wt.getColumnValues(i);
                            if (column_value.size() != 0) {
                                LazoSketch sketch = new LazoSketch(lazo_k);
                                for (Object value : column_value) {
                                    sketch.update(value.toString());
                                }
                                index.insert(wt.getHeaders().get(i), sketch);
                                //System.out.print(".");
                            }
                        }
                    }
                });
                stopTime_index = System.currentTimeMillis();
                index_time = (stopTime_index - startTime_index) / 1000;
                System.out.println("\n" + index_time + " ***done indexing training dataset***");
            }

            Stream<String> test_set = InputReader.parse_wiki_tables_file(Config.TESTING_WIKI_FILENAME);
            System.out.println("***Done reading input files with missing headers ***");
            {
                long startTime_test = 0, stopTime_test = 0, test_time = 0;
                startTime_test=System.currentTimeMillis();
                test_set.parallel().forEach(
                        json_table ->
                        {
                            long startTime = 0, stopTime = 0;
                            startTime=System.currentTimeMillis();
                            float topk_time = 0, coherance_time = 0;
                            boolean resultss = true;

                            //1-get wt table----------------------------------------
                            WTable w_table = WTable.fromString(json_table);

                            //topk for each column
                            Topk_candidates results = new Topk_candidates(Config.k, w_table.getNumCols());

                            //for each column search for results
                            for (int i = 0; i < w_table.getNumCols(); i++) {
                                Set column_value = w_table.getColumnValues(i);
                                if (column_value.size() != 0) {
                                    LazoSketch sketch = new LazoSketch(lazo_k);
                                    for (Object value : column_value) {
                                        sketch.update(value.toString());
                                    }

                                    //2- find topk for each column-------------------------------
                                    Set<LazoIndex.LazoCandidate> candidates = index.queryContainment(sketch, 0);
                                    for (LazoIndex.LazoCandidate current : candidates)
                                        if ( current.key.toString() != null &&
                                                !StringUtils.isBlank(current.key.toString()) &&
                                                current.key.toString().length() <= 25  &&
                                                !current.key.toString().matches("[0-9]+")
                                        )
                                        results.add_candidate(i, new Header_Candidate(current.key.toString().replace(";",""), current.jcx));
                                }
                            }

                            stopTime = System.currentTimeMillis();
                            topk_time = (stopTime - startTime) / 1000;
                            //write_to_disk_phase1(w_table, w_table.getHeaders(), results);
                            //write_to_disk_runtime(w_table, w_table.getHeaders(), topk_time, coherance_time, topk_time + coherance_time);

                            //build a schema candidate
                            List<String> temp = new ArrayList<>();
                            boolean empty=true;
                            for (int i = 0; i < results.getScored_candidates().length; i++) {
                                if (!results.getScored_candidates()[i].isEmpty())
                                    empty=false;
                            }
                            if(empty)
                                write_to_disk(w_table, w_table.getHeaders(), Collections.<String>emptyList(), -2, 1);
                            else {
                                for (int i = 0; i < results.getScored_candidates().length; i++) {
                                    if (results.getScored_candidates()[i].isEmpty()) {
                                        temp.add("NORESULT");
                                        //candidates_list.add(temp);
                                    }
                                    else {
                                        String header_temp = ((Header_Candidate) (results.getScored_candidates()[i].peek())).getHeader().trim().toLowerCase().replaceAll(" ", "_");
                                        temp.add(header_temp);
                                    }
                                }
                                write_to_disk(w_table, w_table.getHeaders(), temp, -1, 1);
                            }
                            //System.out.println("Table: " + w_table.get_id() + "--> done in " + topk_time + "with results? " + resultss);
                        }
                );
                stopTime_test = System.currentTimeMillis();
                test_time = (stopTime_test - startTime_test) / 1000;
                System.out.println("\n" + test_time + " ***done testing***");
            }

        }
    static synchronized void write_to_disk_phase1(WTable wtable,List<String> original,Topk_candidates candidates)
    { List<String> original_schema =  original.stream().map(ee -> ee.replace(" ", "_")).map(ee -> ee.replace("-", "_")).map(ee -> ee.toLowerCase())
            .collect(Collectors.toList());

        for (int i = 0; i < candidates.getScored_candidates().length; i++) {
            List<String> temp = new ArrayList<>();
            if (candidates.getScored_candidates()[i].isEmpty()) {
                temp.add("NORESULT");
            }
            for (Candidate j : candidates.getScored_candidates()[i]) {
                String header_temp = ((Header_Candidate) j).getHeader().trim().toLowerCase().replaceAll(" ", "_");
                temp.add(header_temp);
            }
            ResultWriter.add2Result(wtable.get_id().replace(";", " ") +
                    ";" +
                    wtable.getTableName().replace(";", " ").replace("\n", " ").replace("\r", "") +
                    ";" +
                    wtable.getPgTitle().replace(";", " ").replace("\n", " ").replace("\r", "") +
                    ";" +
                    wtable.getNumDataRows() +
                    ";" +
                    original.size() +
                    ";" +
                    wtable.getNumericColumns().length +
                    ";" +
                    original_schema.get(i) +
                    ";" +
                    String.join("-",temp).replace(";", " ")
                    + "\n", Config.Output.RESULT_PHASE1, "");
        }



    }

    static synchronized void write_to_disk_runtime(WTable wtable,List<String> original, float search, float coherent, float all) {
        ResultWriter.add2Result(wtable.get_id().replace(";", " ") +
                ";" +
                wtable.getTableName().replace(";", " ").replace("\n", " ").replace("\r", "") +
                ";" +
                wtable.getPgTitle().replace(";", " ").replace("\n", " ").replace("\r", "") +
                ";" +
                wtable.getNumDataRows() +
                ";" +
                original.size() +
                ";" +
                wtable.getNumericColumns().length +
                ";" +
                search+
                ";" +
                coherent +
                ";" +
                all + "\n", Config.Output.RUNTIME, "");
    }

    static synchronized void write_to_disk(WTable wtable,List<String> original, List<String> schema, double score, int k) {
        String result_schema = "";
        result_schema = String.join("-", schema.stream().map(ee -> ee.replace(" ", "_")).map(ee -> ee.replace("-", "_"))
                .collect(Collectors.toList()));
        String original_schema = String.join("-", original.stream().map(ee -> ee.replace(" ", "_")).map(ee -> ee.replace("-", "_"))
                .collect(Collectors.toList()));

        ResultWriter.add2Result(wtable.get_id().replace(";", " ") +
                ";" +
                wtable.getTableName().replace(";", " ").replace("\n", " ").replace("\r", "") +
                ";" +
                wtable.getPgTitle().replace(";", " ").replace("\n", " ").replace("\r", "") +
                ";" +
                wtable.getNumDataRows() +
                ";" +
                original.size() +
                ";" +
                wtable.getNumericColumns().length +
                ";" +
                k +
                ";" +
                result_schema +
                ";" +
                original_schema +
                ";" +
                score + "\n", Config.Output.RESULT, "");

    }

}
