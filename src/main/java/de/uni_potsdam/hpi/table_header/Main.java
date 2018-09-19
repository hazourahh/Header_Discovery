package de.uni_potsdam.hpi.table_header;


import de.uni_potsdam.hpi.table_header.data_structures.Similarity_caculator;
import de.uni_potsdam.hpi.table_header.data_structures.hyper_table.HTable;
import de.uni_potsdam.hpi.table_header.io.InputReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;


public class Main {

    public static void main(String[] args) {

        Similarity_caculator calculator = new Similarity_caculator();
        String file_name = "";

        if (args.length > 0) {
            file_name = args[0];
            //read file with missing header
             HTable hyper_table=InputReader.readFile(file_name,",");

            //load or build the webtables sketch if it is not created
            calculator.initialize();

            //calculate the similarity between the input table and all the webtables
             calculator.caculate_similarity( hyper_table,0.1);


        }

    }
}
