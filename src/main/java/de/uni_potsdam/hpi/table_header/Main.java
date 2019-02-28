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


import java.util.stream.Stream;

public class Main {

    public static void main(String[] args) {
        int k=5; //to choose top K candidate for each header
        int m=5; //to choose top m candidate for each schema


        Similarity_caculator calculator = new Similarity_caculator();
        Coherent_Blinder blinder=new Coherent_Blinder();

//--------------------------------Sample and build Htables---------------------
            //1. load or build the webtables sketch if it is not created
           calculator.initialize(true,20); //run it with true only once to generate sample and train from the full dataset

//------------------------------------schema statistics------------------------------
            //2. load or build schemata statistics
            blinder.initialize();
//----------------------------------- Testing ---------------------------------

        //read the file with missing header into hyper representation
         Stream<WTable> test_set= InputReader.parse_wiki_tables_object(Config.TESTING_WIKI_FILENAME);
         System.out.println("***Done reading input files with missing headers ***");


         //calculate the similarity between the input table and all the webtables (output result)
        //TODO: remove saving result to file in the final product
         test_set.forEach(
                 hyper_table->
                 {  //1-convert to htable
                     HTable current= hyper_table.Convert2Hyper();
                     //2- find topk for each header
                     Topk_candidates candidates= calculator.calculate_similarity(current,k);

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

                     blinder.coherant_blind_candidate(candidates,m);
                     MinMaxPriorityQueue<Candidate> result= blinder.getCandidates().getScored_candidates()[0];
                     StringBuilder result_text = new StringBuilder();
                     result.forEach(e->
                     {  Schema_Candidate cand=(Schema_Candidate) e;
                         result_text.append(current.get_id().replace(",", " "));
                         result_text.append(";");
                         result_text.append(cand.getSimilarity_score());
                         result_text.append(";[");
                         result_text.append(String.join("-",cand.getSchema()));
                         result_text.append(";][");
                         result_text.append(String.join("-",current.getHeaders()));
                         result_text.append("]\n");
                         ResultWriter.add2Result(result_text.toString(), Config.Output.RESULT,"");
                      });
                  });

            //stopTime = System.currentTimeMillis();
           // elapsedTime = stopTime - startTime;
           // System.out.println("find top k header for each column "+elapsedTime);
           // startTime = System.currentTimeMillis();
//-------------------------------------------------------------------------------------------

            /*ArrayList<String> c= new ArrayList(Arrays.asList("issue","author"));
            double a=blinder.getSTATISTICDB().get_header_frequency("issue");
            double b=blinder.getSTATISTICDB().get_header_frequency("author");
            double ab=blinder.getSTATISTICDB().get_header_pair_frequency("issue","author");
           double x=blinder.getSTATISTICDB().cohere(c);
*/
            //blinder.coherant_blind_candidate(calculator.getTopk_Candidates(),10);
            //stopTime = System.currentTimeMillis();
           // elapsedTime = stopTime - startTime;
           // System.out.println("find all combinations "+elapsedTime);
           // startTime = System.currentTimeMillis();
           // MinMaxPriorityQueue<Candidate> result= blinder.getCandidates().getScored_candidates()[1];
           // result.forEach(System.out::println);
            // stopTime = System.currentTimeMillis();
            // elapsedTime = stopTime - startTime;
           // System.out.println("find the coherance of one compination "+elapsedTime);
            //Arrays.stream(result).forEach();
       // }

    }

    }
