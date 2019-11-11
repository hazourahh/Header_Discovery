package de.uni_potsdam.hpi.table_header;


import de.uni_potsdam.hpi.table_header.data_structures.Result.Candidate;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Header_Candidate;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Topk_candidates;
import de.uni_potsdam.hpi.table_header.data_structures.wiki_table.WTable;
import de.uni_potsdam.hpi.table_header.io.Config;
import de.uni_potsdam.hpi.table_header.io.InputReader;
import de.uni_potsdam.hpi.table_header.io.ResultWriter;
import lazo.index.LazoIndex;
import lazo.sketch.LazoSketch;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Hazar Harmouch
 */

public class LAZO_test {

        public static void main(String[] args) {
            LazoIndex index = new LazoIndex(1);
            //1- parse training data
            Stream<String> train_tables = InputReader.parse_wiki_tables_file(Config.TRAINING_WIKI_FILENAME);

            train_tables.forEach(json_table -> {
                WTable wt = WTable.fromString(json_table);

                //filter out low quality tables
                if (!wt.has_missing_header_line())
                {
                    for (int i = 0; i < wt.getNumCols(); i++) {
                        Set column_value = wt.getColumnValues(i);
                        if (column_value.size() != 0) {
                            LazoSketch sketch = new LazoSketch(1);
                            for (Object value : column_value) {
                                sketch.update(value.toString());
                            }
                            index.insert(wt.getHeaders().get(i), sketch);
                        }
                    }
                }
            });

            System.out.println("***done indexing training dataset***");


            Stream<String> test_set = InputReader.parse_wiki_tables_file(Config.TESTING_WIKI_FILENAME);
            System.out.println("***Done reading input files with missing headers ***");

            test_set.forEach(
                    json_table ->
                    {
                        long startTime = 0, stopTime = 0;
                        float topk_time = 0, coherance_time = 0;
                        boolean resultss = true;

                        //1-get wt table----------------------------------------
                        WTable w_table = WTable.fromString(json_table);

                        //topk for each column
                        Topk_candidates results = new Topk_candidates(Config.k,  w_table.getNumCols());

                        //for each column search for results
                        for (int i = 0; i < w_table.getNumCols(); i++) {
                            Set column_value = w_table.getColumnValues(i);
                            if (column_value.size() != 0) {
                                LazoSketch sketch = new LazoSketch(1);
                                for (Object value : column_value) {
                                    sketch.update(value.toString());
                                }

                                //2- find topk for each column-------------------------------
                                startTime = System.currentTimeMillis();
                                Set<LazoIndex.LazoCandidate> candidates = index.queryContainment(sketch, 0);
                                for(LazoIndex.LazoCandidate current: candidates)
                                    results.add_candidate(i, new Header_Candidate(current.key.toString(), current.jcx));
                            }
                        }

                                stopTime = System.currentTimeMillis();
                                topk_time = (stopTime - startTime) / 1000;
                                write_to_disk_phase1(w_table,w_table.getHeaders(), results);
                                write_to_disk_runtime(w_table, w_table.getHeaders(), topk_time, coherance_time, topk_time + coherance_time);
                                System.out.println("Table: " + w_table.get_id() + "--> done in " + topk_time +  "with results? " + resultss);
                    }
                    );
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
                    String.join("-",temp)
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

}
