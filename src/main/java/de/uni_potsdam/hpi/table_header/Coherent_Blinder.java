package de.uni_potsdam.hpi.table_header;


import de.uni_potsdam.hpi.table_header.data_structures.Result.Topk_candidates;

import de.uni_potsdam.hpi.table_header.data_structures.statistics_db.ACSDb;
import de.uni_potsdam.hpi.table_header.io.Config;
import de.uni_potsdam.hpi.table_header.io.InputReader;
import de.uni_potsdam.hpi.table_header.io.Serializer;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


class Coherent_Blinder {

    private ACSDb STATISTICDB=new ACSDb();

    public ACSDb getSTATISTICDB() {
        return STATISTICDB;
    }

    //filters??
    //TODO : filter long attribute of schemata happens once?


    void initialize() {
        // check if the it is already stored to load it and if not re-create it
        try {
              STATISTICDB= (ACSDb) Serializer.deserialize(Config.ACSDB_FILENAME);
              System.out.println("***done deserialize acsdb***");

        } catch (FileNotFoundException e) {

            //parse acsdb
            STATISTICDB=parse_ACSDb();

            // store acsdb
            store_ACSDB();

            System.out.println("***done building acsdb***");

        } catch (IOException e) {
            System.err.println("Could not de-serialize acsdb");
            e.printStackTrace();
            System.exit(1);
        } catch (ClassNotFoundException e) {
            System.err.println("Could not cast the de-serialized acsdb");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private ACSDb parse_ACSDb()
    {
        return InputReader.read_ACSDB_File(Config.Input_acsdb_file);
    }

    private void store_ACSDB() {
        try {
            Serializer.serialize(STATISTICDB, Config.ACSDB_FILENAME);
        } catch (IOException e) {
            System.err.println("Could not save the ACSDB to the disk");
            e.printStackTrace();
            System.exit(1);
        }

    }



    //TODO: try to prune the search space hear top k-m search idea
    void coherant_blind_candidate(Set<String>[] candidates, Map<List<String>,Double> result, int depth, List<String> current)
    {
        //List<List<String>> cand = Arrays.stream(candidates).map((e->new ArrayList<String>(e)).collect(Collectors.toList());
        if(depth == candidates.length)
        {
            result.put(current,STATISTICDB.cohere(current));
            return;
        }

        for(int i = 0; i < candidates[depth].size(); ++i)
        {   current.add(candidates[depth].iterator().next());
            coherant_blind_candidate(candidates, result, depth + 1, current );
        }


    }
}
