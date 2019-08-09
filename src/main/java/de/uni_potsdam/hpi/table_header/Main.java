package de.uni_potsdam.hpi.table_header;


import com.google.common.collect.MinMaxPriorityQueue;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Candidate;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Header_Candidate;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Schema_Candidate;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Topk_candidates;
import de.uni_potsdam.hpi.table_header.data_structures.hyper_table.HTable;
import de.uni_potsdam.hpi.table_header.data_structures.wiki_table.WTable;
import de.uni_potsdam.hpi.table_header.io.Config;
import de.uni_potsdam.hpi.table_header.io.InputReader;
import de.uni_potsdam.hpi.table_header.io.ResultWriter;
import org.aksw.palmetto.aggregation.ArithmeticMean;
import org.aksw.palmetto.calculations.direct.CondProbConfirmationMeasure;
import org.aksw.palmetto.subsets.OnePreceding;
import org.aksw.palmetto.subsets.OneSucceeding;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Hazar Harmouch
 */
public class Main {

    public static void main(String[] args) {

        //----------important----------------
        //adjust all parameters in Config.java file before runing the experiments
        //delete the hypertable file if you changed the train data

        //--------------------------------Sample and build Htables and the index---------------------
        //2. load or build the webtables sketch if it is not created
        //calculator.initialize(true,25); //run it with true only once to generate sample and train from the full dataset
        if(Config.test_type==Config.testdata.OPENDATA)
        {Config.TRAINING_WIKI_FILENAME=Config.FULL_WIKI_FILENAME;}
        Similarity_caculator.initialize(false, 5);
       Coherent_Blinder blinder = new Coherent_Blinder(new OneSucceeding(), new CondProbConfirmationMeasure(), new ArithmeticMean());

//----------------------------------- Testing ---------------------------------
        ResultWriter.add2Result("[k:"+Config.k+
                                    ",m:"+Config.m+
                                    ",table_similarity:"+Config.table_similarity+
                                    ",column_similarity:"+ Config.column_similarity+
                                    ",table_similarity_weighting:" + Config.table_similarity_weighting+
                                    ",table_similarity_filtering:" + Config.table_similarity_filtering+
                                     ",table_similarity_weight:" + Config.table_similarity_weight+
                                     ",testdata:" + Config.test_type+
                                     "]\n", Config.Output.RESULT, "");

if(Config.test_type==Config.testdata.OPENDATA)  // input is csv files
{
    File folder = new File(Config.inputFolderPath);
    File[] listOfFiles = folder.listFiles();
    Arrays.stream(listOfFiles).forEach( file-> {
        //  for(int i=0;i<listOfFiles.length;i++) {
        String table_name = file.getPath();
       //String table_name="data\\Oahu_TEFAP_Agencies.csv";
        //read the file with missing header into hyper representation
        HTable current = InputReader.read_WT_File(table_name, ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", true);
        if (current == null)
        { write_to_disk_od(table_name, Collections.<String>emptyList(), Collections.<String>emptyList(), -4, 1);}
        else{
            //2- find topk for each header-------------------------------
            Topk_candidates candidates = Similarity_caculator.calculate_similarity(current, Config.k);
            //write_to_disk_phase1(w_table, current.getHeaders(), candidates);


            //3-coherance blind ---------------------------------------
            if (candidates.getScored_candidates().length == 0) {
                write_to_disk_od(table_name, current.getHeaders(), Collections.<String>emptyList(), -1, 1);
            } else {

                try {

                    Topk_candidates schema_candidates = blinder.coherant_blind_candidate(candidates, Config.m);

                    MinMaxPriorityQueue<Candidate> result = schema_candidates.getScored_candidates()[0];
                    AtomicInteger counter = new AtomicInteger(1);
                    result.forEach(e ->
                    {
                        Schema_Candidate cand = (Schema_Candidate) e;
                        write_to_disk_od(table_name, current.getHeaders(), cand.getSchema(), cand.getSimilarity_score(), counter.getAndIncrement());

                        // System.out.println(current.get_id().replace(",", " ")+";"+String.join("-",cand.getSchema())+";"+String.join("-",current.getHeaders())+";"+cand.getSimilarity_score()+"\n");
                    });
                    if (result.isEmpty()) {
                        write_to_disk_od(table_name, current.getHeaders(), Collections.<String>emptyList(), -2, 1);
                    }
                } catch (Exception e) {

                    System.err.println("something went wrong with table" + table_name);
                    write_to_disk_od(table_name, current.getHeaders(), Collections.<String>emptyList(), -3, 1);
                    // e.printStackTrace();
                }
            }
        }
            System.out.println("Table: " + table_name + "--> done");

    });
    //}

}
else  //input is a JSON file with table per line
    {
    //read the file with missing header into hyper representation
    Stream<String> test_set = InputReader.parse_wiki_tables_file(Config.TESTING_WIKI_FILENAME);
    System.out.println("***Done reading input files with missing headers ***");


    //calculate the similarity between the input table and all the webtables (output result)
    test_set.parallel().forEach(
            json_table ->
            {
                long startTime = 0, stopTime = 0;
                float topk_time = 0, coherance_time = 0;
                boolean resultss = true;

                //1-convert to htable----------------------------------------
                WTable w_table = WTable.fromString(json_table);
                HTable current = w_table.Convert2Hyper();


                //2- find topk for each header-------------------------------
                startTime = System.currentTimeMillis();

                Topk_candidates candidates = Similarity_caculator.calculate_similarity(current, Config.k);
                //write_to_disk_phase1(w_table, current.getHeaders(), candidates);
                stopTime = System.currentTimeMillis();
                topk_time = (stopTime - startTime) / 1000;

                //3-coherance blind ---------------------------------------
                if (candidates.getScored_candidates().length == 0) {
                    resultss = false;
                    write_to_disk(w_table, current.getHeaders(), Collections.<String>emptyList(), -1, 1);
                } else {

                    try {

                        startTime = System.currentTimeMillis();

                        Topk_candidates schema_candidates = blinder.coherant_blind_candidate(candidates, Config.m);

                        stopTime = System.currentTimeMillis();
                        coherance_time = (stopTime - startTime) / 1000;


                        MinMaxPriorityQueue<Candidate> result = schema_candidates.getScored_candidates()[0];
                        AtomicInteger counter = new AtomicInteger(1);
                        result.forEach(e ->
                        {
                            Schema_Candidate cand = (Schema_Candidate) e;
                            write_to_disk(w_table, current.getHeaders(), cand.getSchema(), cand.getSimilarity_score(), counter.getAndIncrement());

                            // System.out.println(current.get_id().replace(",", " ")+";"+String.join("-",cand.getSchema())+";"+String.join("-",current.getHeaders())+";"+cand.getSimilarity_score()+"\n");
                        });
                        if (result.isEmpty()) {
                            write_to_disk(w_table, current.getHeaders(), Collections.<String>emptyList(), -2, 1);
                        }
                    } catch (Exception e) {

                        System.err.println("something went wrong with table" + w_table.get_id());
                        write_to_disk(w_table, current.getHeaders(), Collections.<String>emptyList(), -3, 1);
                        // e.printStackTrace();
                    }
                }

                System.out.println("Table: " + current.get_id() + "--> done in " + topk_time + "/" + coherance_time + "with results? " + resultss);

            }
    );
}
        //stopTime = System.currentTimeMillis();
        // elapsedTime = stopTime - startTime;
        // startTime = System.currentTimeMillis();

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

    static synchronized void write_to_disk_od(String table_name,List<String> original, List<String> schema, double score, int k) {
        String result_schema = "";
        result_schema = String.join("-", schema.stream().map(ee -> ee.replace(" ", "_")).map(ee -> ee.replace("-", "_"))
                .collect(Collectors.toList()));
        String original_schema = String.join("-", original.stream().map(ee -> ee.replace(" ", "_")).map(ee -> ee.replace("-", "_"))
                .collect(Collectors.toList()));

        ResultWriter.add2Result("" +
                ";" +
                table_name+
                ";" +
                "" +
                ";" +
                "" +
                ";" +
                original.size() +
                ";" +
                "" +
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
