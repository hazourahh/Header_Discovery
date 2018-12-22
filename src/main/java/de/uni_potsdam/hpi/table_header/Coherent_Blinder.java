package de.uni_potsdam.hpi.table_header;


import de.uni_potsdam.hpi.table_header.data_structures.Result.Header_Candidate;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Schema_Candidate;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Topk_candidates;

import de.uni_potsdam.hpi.table_header.data_structures.statistics_db.ACSDb;
import de.uni_potsdam.hpi.table_header.io.Config;
import de.uni_potsdam.hpi.table_header.io.InputReader;
import de.uni_potsdam.hpi.table_header.io.Serializer;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;


class Coherent_Blinder {

    private ACSDb STATISTICDB=new ACSDb();
    private Topk_candidates candidates;

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

    public Topk_candidates getCandidates() {
        return candidates;
    }

    //TODO: try to prune the search space hear top k-m search idea
  public void coherant_blind_candidate(Set<String>[] list, int k )
    {  //build permutations
        List<List<String>> result = new ArrayList<List<String>>();
        int numSets = list.length;
        String[] tmpResult = new String[numSets];
        cartesian(list, 0, tmpResult, result);
        candidates=new Topk_candidates(k,1);
        //caculate top k coherent candidate list
           result.forEach(e->candidates.add_candidate(0, new Schema_Candidate(e,STATISTICDB.cohere(e))));
    }


   private void cartesian(Set<String>[] list, int n, String[] tmpResult, List<List<String>> result)
    {
        if (n == list.length) {
            //if(STATISTICDB.cohere(Arrays.asList(tmpResult))>0.5)
            {
                result.add(new ArrayList<String>(Arrays.asList(tmpResult)));
            }
            return;
        }

        for (String i : list[n]) {
            tmpResult[n] = i;
            cartesian(list, n + 1, tmpResult, result);
        }
    }
}
