package de.uni_potsdam.hpi.table_header;

import de.uni_potsdam.hpi.table_header.data_structures.Result.Candidate;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Header_Candidate;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Schema_Candidate;
import de.uni_potsdam.hpi.table_header.data_structures.Result.Topk_candidates;
import de.uni_potsdam.hpi.table_header.io.Config;
import de.uni_potsdam.hpi.table_header.io.InputReader;
import org.aksw.palmetto.Coherence;
import org.aksw.palmetto.DirectConfirmationBasedCoherence;
import org.aksw.palmetto.Palmetto;
import org.aksw.palmetto.aggregation.Aggregation;
import org.aksw.palmetto.calculations.direct.DirectConfirmationMeasure;
import org.aksw.palmetto.corpus.CorpusAdapter;
import org.aksw.palmetto.corpus.lucene.LuceneCorpusAdapter;
import org.aksw.palmetto.prob.bd.BooleanDocumentProbabilitySupplier;
import org.aksw.palmetto.subsets.Segmentator;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;


class Coherent_Blinder {

    //private ACSDb STATISTICDB=new ACSDb();

    private Topk_candidates schema_candidates;
    private Coherence coherence;

    /* public ACSDb getSTATISTICDB() {
        return STATISTICDB;
    }*/

    //filters??
    //TODO : filter long attribute of schemata happens once?


    void initialize(Segmentator segmentation, DirectConfirmationMeasure confirmation, Aggregation aggregation) {

        try {
            // STATISTICDB= (ACSDb) Serializer.deserialize(Config.ACSDB_FILENAME);
            File indexpath = new File(Config.index_Folder);
            IndexReader reader = DirectoryReader.open(FSDirectory.open(indexpath));
            //IndexSearcher searcher=new IndexSearcher(reader);
            //Analyzer analyzer= new WhitespaceAnalyzer();
            // QueryParser pars=
            reader.close();


            System.out.println("***acsdb index exists***");

        } catch (FileNotFoundException e) {

            //parse acsdb
            //STATISTICDB=parse_ACSDb();
            InputReader.build_ACSDB_index(Config.Input_acsdb_file);
            // store acsdb
            //store_ACSDB();
            System.out.println("***done building acsdb index***");

        } catch (IOException e) {
            System.err.println("Could not open acsdb index");
            e.printStackTrace();
            System.exit(1);
        }
        try {
            CorpusAdapter corpusAdapter = LuceneCorpusAdapter.create(Config.index_Folder, Palmetto.DEFAULT_TEXT_INDEX_FIELD_NAME);
            coherence = new DirectConfirmationBasedCoherence(segmentation,
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    confirmation, aggregation);
            if (coherence == null) {
                return;
            }
        } catch (IOException e) {
            System.err.println("Could not open acsdb index");
            e.printStackTrace();
            System.exit(1);
        }
        //corpusAdapter.close();

    }


//----------------------------------------------------------------

    public Topk_candidates getCandidates() {
        return schema_candidates;
    }


//----------------------------------------------------------------
    //TODO: try to prune the search space here top k-m search idea
     /*private ACSDb parse_ACSDb()
    {
        return InputReader.read_ACSDB_File(Config.Input_acsdb_file);
    }*/

   /* private void store_ACSDB() {
        try {
            Serializer.serialize(STATISTICDB, Config.ACSDB_FILENAME);
        } catch (IOException e) {
            System.err.println("Could not save the ACSDB to the disk");
            e.printStackTrace();
            System.exit(1);
        }

    }*/

    public void coherant_blind_candidate(Topk_candidates candidates, int k) {  //build permutations

        List<String[]> result = new ArrayList<>();
        String[] tmpResult = new String[candidates.getScored_candidates().length];
        cartesian(candidates, 0, tmpResult, result);

        // process all strings to have _ instead space to mach the index
        int i = 0;
        String[][] candidate_array = new String[result.size()][];
        for (String[] schema_candidate : result) {
            String[] schema_candidate_prep = Arrays.stream(schema_candidate)
                    .map(String::trim)
                    .map(String::toLowerCase)
                    .map(str -> str.replaceAll(" ", "_"))
                    .toArray(String[]::new);
            candidate_array[i++] = schema_candidate_prep;
        }

        //caculate top k coherent candidate list
        //result.forEach(e->schema_candidates.add_candidate(0, new Schema_Candidate(e,STATISTICDB.cohere(e))));
        schema_candidates = new Topk_candidates(k, 1);
        double coherences[] = coherence.calculateCoherences(candidate_array);
        int j = 0;
        for (String[] schema_candidate : candidate_array) {
            if ((schema_candidate != null) && (schema_candidate.length > 0)) {

                schema_candidates.add_candidate(0,
                        new Schema_Candidate(Arrays.asList(schema_candidate), coherences[j++]));
            }
        }

    }


    private void cartesian(Topk_candidates list, int n, String[] tmpResult, List<String[]> result) {
        if (n == list.getScored_candidates().length) {
            //if(STATISTICDB.cohere(Arrays.asList(tmpResult))>0.5)
            List<String> temp = Arrays.asList(tmpResult);
            if (containsUnique(temp) && !result.contains(temp)) {
                result.add(tmpResult);
            }
            return;
        }
        //case of no candidate is founded
        if (list.getScored_candidates()[n].isEmpty()) {
            tmpResult[n] = "";
            cartesian(list, n + 1, tmpResult, result);
        } else
            for (Candidate i : list.getScored_candidates()[n]) {
                tmpResult[n] = ((Header_Candidate) i).getHeader();
                cartesian(list, n + 1, tmpResult, result);
            }
    }


    private <T> boolean containsUnique(List<T> list) {
        return list.stream().allMatch(new HashSet<>()::add);
    }

}
