package de.uni_potsdam.hpi.table_header;


import com.google.common.collect.MinMaxPriorityQueue;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Candidate;
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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
/**
 * @author Hazar Harmouch
 *
 */
public class Main {

    public static void main(String[] args) {
        int k=10; //to choose top K candidate for each header
        int m=10; //to choose top m candidate for each schema
//------------------------------------schema statistics------------------------------



        //--------------------------------Sample and build Htables---------------------
        //2. load or build the webtables sketch if it is not created
        //calculator.initialize(true,25); //run it with true only once to generate sample and train from the full dataset
       Similarity_caculator.initialize(true,5);

//----------------------------------- Testing ---------------------------------

        //read the file with missing header into hyper representation
         Stream<String> test_set= InputReader.parse_wiki_tables_file(Config.TESTING_WIKI_FILENAME);
         System.out.println("***Done reading input files with missing headers ***");


         //calculate the similarity between the input table and all the webtables (output result)
        //TODO: remove saving result to file in the final product

         test_set.parallel().forEach(
                 json_table->
                 {
                     //------------------------------------------------
                     Coherent_Blinder blinder=new Coherent_Blinder(new OnePreceding(),new CondProbConfirmationMeasure(),new ArithmeticMean());

                     long startTime = System.currentTimeMillis();
                     //1-convert to htable
                     WTable w_table = WTable.fromString(json_table);
                     HTable current = w_table.Convert2Hyper();
                     System.out.print("Table: " + current.get_id());
                     //2- find topk for each header
                     Topk_candidates candidates = Similarity_caculator.calculate_similarity(current, k);

                     /*StringBuilder result = new StringBuilder();
                     for(int i=0;i<candidates.getScored_candidates().length; i++) {
                         result.append(current.get_id().replace(",", " "));
                         result.append(",");
                         result.append(current.getName().replace(",", " "));
                         result.append(",");
                         result.append(current.getColumns().get(i).getLabel().replace(",", " "));
                         result.append(",");
                         candidates.getScored_candidates()[i].forEach( e->{
                             Header_Candidate can=(Header_Candidate) e;
                             result.append( can.getHeader().replace(",", " ")+"("+ can.getSimilarity_score()+"),");});
                         result.append("\n");
                     }
                     ResultWriter.add2Result(result.toString(), Config.Output.RESULT,"");
                     */

                   if (candidates.getScored_candidates().length == 0) {
                       write_to_disk(w_table,null,-1,1);
                     } else {
                         try {
                             Topk_candidates schema_candidates= blinder.coherant_blind_candidate(candidates, m);
                             MinMaxPriorityQueue<Candidate> result = schema_candidates.getScored_candidates()[0];
                             AtomicInteger counter = new AtomicInteger(1);
                             result.forEach(e ->
                             {
                                 Schema_Candidate cand = (Schema_Candidate) e;
                                 write_to_disk(w_table, cand.getSchema(),cand.getSimilarity_score(),counter.getAndIncrement());

                                 // System.out.println(current.get_id().replace(",", " ")+";"+String.join("-",cand.getSchema())+";"+String.join("-",current.getHeaders())+";"+cand.getSimilarity_score()+"\n");
                             });
                             if (result.isEmpty())
                                 write_to_disk(w_table,null,-2,1);
                         } catch (Exception e) {
                             System.err.println("something went wrong");
                         }

                     }
                     long stopTime = System.currentTimeMillis();
                     System.out.println("--> done in "+ (stopTime - startTime)/1000);
                  }
                  );

            //stopTime = System.currentTimeMillis();
           // elapsedTime = stopTime - startTime;
           // startTime = System.currentTimeMillis();

    }


   static synchronized void write_to_disk(WTable wtable, List<String> schema,double score, int k)
    {
        String result_schema="";
        result_schema= String.join("-", schema.stream().map(ee -> ee.replace(" ", "_")).map(ee -> ee.replace("-", "_"))
                .collect(Collectors.toList()));
        String original_schema=String.join("-", wtable.getHeaders().stream().map(ee -> ee.replace(" ", "_")).map(ee -> ee.replace("-", "_"))
                .collect(Collectors.toList()));

        ResultWriter.add2Result(wtable.get_id().replace(";", " ") +
            ";" +
            wtable.getTableName().replace(";", " ").replace("\n", " ").replace("\r", "")+
            ";" +
            wtable.getPgTitle().replace(";", " ").replace("\n", " ").replace("\r", "")+
            ";" +
            wtable.getNumDataRows() +
            ";" +
            wtable.getNumCols() +
            ";" +
            wtable.getNumericColumns().length +
            ";" +
                k+
             ";" +
                result_schema+
            ";" +
             original_schema+
            ";" +
            score + "\n", Config.Output.RESULT, "");}
    }
