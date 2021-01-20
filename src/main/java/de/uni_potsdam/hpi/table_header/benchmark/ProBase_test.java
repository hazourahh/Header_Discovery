package de.uni_potsdam.hpi.table_header.benchmark;


import de.uni_potsdam.hpi.table_header.data_structures.Result.Candidate;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Header_Candidate;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Topk_candidates;
import de.uni_potsdam.hpi.table_header.data_structures.wiki_table.WTable;
import de.uni_potsdam.hpi.table_header.io.Config;
import de.uni_potsdam.hpi.table_header.io.InputReader;
import de.uni_potsdam.hpi.table_header.io.ResultWriter;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.URI;
import java.net.http.HttpResponse;

import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Hazar Harmouch
 */

public class ProBase_test {

        public static void main(String[] args) {
             int topk_concept=5;// the top-k concept that we will use for each value to fidn the concept that represent the column

            //disable SSL and host verification
           // final Properties props = System.getProperties();
           // props.setProperty("jdk.internal.httpclient.disableHostnameVerification", Boolean.TRUE.toString());
           // SSLFix.execute();
            HttpClient client = HttpClient.newHttpClient();


            Stream<String> test_set = InputReader.parse_wiki_tables_file(Config.TESTING_WIKI_FILENAME);
            System.out.println("***Done reading input files with missing headers ***");
            {
                long startTime_test = 0, stopTime_test = 0, test_time = 0;
                startTime_test=System.currentTimeMillis();
                test_set.forEach(
                        json_table ->
                        {
                            long startTime = 0, stopTime = 0;
                            startTime=System.currentTimeMillis();
                            float topk_time = 0, coherance_time = 0;
                            boolean resultss = true;

                            //1-get wt table----------------------------------------
                            WTable w_table = WTable.fromString(json_table);

                            //2-build the topk queue for each column
                            Topk_candidates results = new Topk_candidates(Config.k, w_table.getNumCols());

                            //for each column search for concepts
                            for (int i = 0; i < w_table.getNumCols(); i++) {
                                Set column_value = w_table.getColumnValues(i);
                                if (column_value.size() != 0) {
                                    //for each value in the column find top concepts and scores from probase
                                    for (Object value : column_value) {
                                        //build the get request
                                        String clean_value = value.toString().trim().replaceAll("\\p{Punct}", "").trim().replace(" ", "+");
                                        if (!clean_value.isEmpty() && clean_value.length()<50 && isPureAscii(clean_value)) {
                                            String url = "https://concept.research.microsoft.com/api/Concept/ScoreByProb?instance=";
                                            url += clean_value + "&topK=" + topk_concept;
                                            System.out.println(url);
                                            HttpRequest request = HttpRequest.newBuilder()
                                                    .uri(URI.create(url))
                                                    .build();
                                            HttpResponse<String> response = null;
                                            try {
                                                response = client.send(request,
                                                        HttpResponse.BodyHandlers.ofString());

                                                JSONObject jsonObject = new JSONObject(response.body());

                                                if (jsonObject.length() > 0) {
                                                    System.out.println(response.body());
                                                    for (String concept : jsonObject.keySet()) {
                                                        results.add_candidate(i, new Header_Candidate(concept.replace(" ", "_"), jsonObject.getDouble(concept)));
                                                    }
                                                }

                                            } catch (IOException e) {
                                                e.printStackTrace();
                                            } catch (InterruptedException e) {
                                                e.printStackTrace();
                                            }


                                            //results.add_candidate(i, new Header_Candidate(current.key.toString().replace(";",""), current.jcx));
                                        }
                                    }
                                }
                            }

                            stopTime = System.currentTimeMillis();
                            topk_time = (stopTime - startTime) / 1000;
                            write_to_disk_phase1(w_table, w_table.getHeaders(), results);
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

    public static boolean isPureAscii(String v) {
        return Charset.forName("US-ASCII").newEncoder().canEncode(v);}
}



