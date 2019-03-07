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
import org.aksw.palmetto.calculations.direct.FitelsonConfirmationMeasure;
import org.aksw.palmetto.subsets.OneOne;


import java.util.stream.Stream;
/**
 * @author Hazar Harmouch
 *
 */
public class Main {

    public static void main(String[] args) {
        int k=5; //to choose top K candidate for each header
        int m=5; //to choose top m candidate for each schema


        Similarity_caculator calculator = new Similarity_caculator();
        Coherent_Blinder blinder=new Coherent_Blinder();

//--------------------------------Sample and build Htables---------------------
            //1. load or build the webtables sketch if it is not created
           calculator.initialize(true,25); //run it with true only once to generate sample and train from the full dataset

//------------------------------------schema statistics------------------------------
            //2. load or build schemata statistics
            blinder.initialize(new OneOne(),new FitelsonConfirmationMeasure(),new ArithmeticMean());
//----------------------------------- Testing ---------------------------------

        //read the file with missing header into hyper representation
         Stream<WTable> test_set= InputReader.parse_wiki_tables_object(Config.TESTING_WIKI_FILENAME);
         System.out.println("***Done reading input files with missing headers ***");


         //calculate the similarity between the input table and all the webtables (output result)
        //TODO: remove saving result to file in the final product
         test_set.forEach(
                 hyper_table->
                 {
                     //1-convert to htable
                     HTable current= hyper_table.Convert2Hyper();
                     System.out.print("Table: "+current.get_id());
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
                    if(candidates.getScored_candidates().length==0)
                    { ResultWriter.add2Result(current.get_id().replace(";", " ")+
                            ";"+
                            current.getName().replace(";", " ")+
                            ";"+
                            hyper_table.getPgTitle().replace(";", " ")+
                            ";"+
                            hyper_table.getNumDataRows()+
                            ";"+
                            hyper_table.getNumCols()+
                            ";"+
                            hyper_table.getNumericColumns().length+
                            ";"+
                            ""+
                            ";"+
                            String.join("-",current.getHeaders())+
                            ";"+
                            -1+"\n", Config.Output.RESULT,"");
                    }
                    else {
                        blinder.coherant_blind_candidate(candidates, m);
                        MinMaxPriorityQueue<Candidate> result = blinder.getCandidates().getScored_candidates()[0];
                        StringBuilder result_text = new StringBuilder();
                        result.forEach(e ->
                        {
                            Schema_Candidate cand = (Schema_Candidate) e;
                            ResultWriter.add2Result(current.get_id().replace(";", " ") +
                                    ";" +
                                    current.getName().replace(";", " ") +
                                    ";" +
                                    hyper_table.getPgTitle().replace(";", " ") +
                                    ";" +
                                    hyper_table.getNumDataRows() +
                                    ";" +
                                    hyper_table.getNumCols() +
                                    ";" +
                                    hyper_table.getNumericColumns().length +
                                    ";" +
                                    String.join("-", cand.getSchema()) +
                                    ";" +
                                    String.join("-", current.getHeaders()) +
                                    ";" +
                                    cand.getSimilarity_score() + "\n", Config.Output.RESULT, "");
                            // System.out.println(current.get_id().replace(",", " ")+";"+String.join("-",cand.getSchema())+";"+String.join("-",current.getHeaders())+";"+cand.getSimilarity_score()+"\n");
                        });
                        if (result.isEmpty())
                            ResultWriter.add2Result(current.get_id().replace(";", " ") +
                                    ";" +
                                    current.getName().replace(";", " ") +
                                    ";" +
                                    hyper_table.getPgTitle().replace(";", " ") +
                                    ";" +
                                    hyper_table.getNumDataRows() +
                                    ";" +
                                    hyper_table.getNumCols() +
                                    ";" +
                                    hyper_table.getNumericColumns().length +
                                    ";" +
                                    "" +
                                    ";" +
                                    String.join("-", current.getHeaders()) +
                                    ";" +
                                    -2 + "\n", Config.Output.RESULT, "");
                    }
                     System.out.println("--> done");
                  });

            //stopTime = System.currentTimeMillis();
           // elapsedTime = stopTime - startTime;
           // startTime = System.currentTimeMillis();

    }

    }
