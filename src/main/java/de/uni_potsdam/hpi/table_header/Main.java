package de.uni_potsdam.hpi.table_header;



import de.uni_potsdam.hpi.table_header.data_structures.hyper_table.HTable;
import de.uni_potsdam.hpi.table_header.io.InputReader;

import java.util.ArrayList;


public class Main {

    public static void main(String[] args) {
        int k=10; //to choose top K candidate
        Similarity_caculator calculator = new Similarity_caculator();
        Coherent_Blinder blinder=new Coherent_Blinder();

        String file_name ;

        if (args.length > 0) {
            //the name of the file with missing header
            file_name = args[0];

            //read the file with missing header into hyper represntation
             HTable hyper_table=InputReader.read_WT_File(file_name,",");

            //1. load or build the webtables sketch if it is not created
            calculator.initialize();
            //2. load or build schemata statistics
            blinder.initialize();
            //calculate the similarity between the input table and all the webtables (output result)
            //TODO: remove saving result in the final product
             calculator.calculate_similarity(hyper_table,k);

            // calculator.getCandidates().print();
            ArrayList results=new ArrayList();
            blinder.coherant_blind_candidate(calculator.getCandidates(),results,0,"");
           results.forEach(System.out::print);
        }
        else
        {System.out.print("No input file");
        System.exit(1);}

    }

    }
