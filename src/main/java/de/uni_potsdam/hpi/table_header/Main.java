package de.uni_potsdam.hpi.table_header;



import com.google.common.collect.MinMaxPriorityQueue;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Candidate;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Schema_Candidate;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Topk_candidates;
import de.uni_potsdam.hpi.table_header.data_structures.hyper_table.HTable;
import de.uni_potsdam.hpi.table_header.io.InputReader;

import java.util.*;


public class Main {

    public static void main(String[] args) {
        int k=2; //to choose top K candidate
        Similarity_caculator calculator = new Similarity_caculator();
        Coherent_Blinder blinder=new Coherent_Blinder();

        String file_name ;

        if (args.length > 0) {
            //the name of the file with missing header
            file_name = args[0];

            long startTime = System.currentTimeMillis();

            //read the file with missing header into hyper represntation
             HTable hyper_table=InputReader.read_WT_File(file_name,",");

            //1. load or build the webtables sketch if it is not created
            calculator.initialize();

            //2. load or build schemata statistics
            blinder.initialize();

            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            System.out.println("initalization time:"+elapsedTime);
            startTime = System.currentTimeMillis();

            //calculate the similarity between the input table and all the webtables (output result)
            //TODO: remove saving result to file in the final product
            calculator.calculate_similarity(hyper_table,k);

            stopTime = System.currentTimeMillis();
            elapsedTime = stopTime - startTime;
            System.out.println("find top k header for each column "+elapsedTime);
            startTime = System.currentTimeMillis();


            blinder.coherant_blind_candidate(calculator.get_unique_Topk_Candidates(),100);
            stopTime = System.currentTimeMillis();
            elapsedTime = stopTime - startTime;
            System.out.println("find all combinations "+elapsedTime);
            startTime = System.currentTimeMillis();

            MinMaxPriorityQueue<Candidate>[] result= blinder.getCandidates().getScored_candidates();

        /*    double score=blinder.getSTATISTICDB().cohere(new ArrayList<String>(
                    Arrays.asList("issue","year")));*/

            stopTime = System.currentTimeMillis();
            elapsedTime = stopTime - startTime;
            System.out.println("find the coherance of one compination "+elapsedTime);

            //Arrays.stream(result).forEach();
        }
        else
        {System.out.print("No input file");
        System.exit(1);}

    }

    }
