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
import org.apache.jena.ext.com.google.common.collect.Lists;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
/**
 * @author Hazar Harmouch
 *
 */

class Coherent_Blinder {


    private Topk_candidates schema_candidates;
    private Coherence coherence;
    public Topk_candidates getCandidates() {
        return schema_candidates;
    }

    //filters??
    //TODO : filter long attribute of schemata happens once?

    /***
     *   build inverted index and coherence measure to evaluate the coherence of the schema candidates
      * @param segmentation
     * @param confirmation
     * @param aggregation
     */
    void initialize(Segmentator segmentation, DirectConfirmationMeasure confirmation, Aggregation aggregation) {

        try { //check if the index already there
               File indexpath = new File(Config.index_Folder);
                IndexReader reader = DirectoryReader.open(FSDirectory.open(indexpath));
                reader.close();
                System.out.println("***acsdb index exists***");

        } catch (FileNotFoundException e) {

            //parse acsdb and build an index
            InputReader.build_ACSDB_index(Config.Input_acsdb_file);
            System.out.println("***done building acsdb index***");

        } catch (IOException e) {
            System.err.println("Could not open acsdb index");
            e.printStackTrace();
            System.exit(1);
        }
        try {
            //build a coherence object based on the input
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


    public void coherant_blind_candidate(Topk_candidates candidates, int k) {
        //build permutations and process all strings to have _ instead space to mach the index
        List<List<String>> candidates_list=new ArrayList<>();
        for(int i=0;i<candidates.getScored_candidates().length;i++)
        {
            List<String> temp= new ArrayList<>();
            if(candidates.getScored_candidates()[i].isEmpty())
            {temp.add("");
             candidates_list.add(temp);
            }
            for (Candidate j : candidates.getScored_candidates()[i]) {
                String header_temp= ((Header_Candidate) j).getHeader().trim().toLowerCase().replaceAll(" ", "_");
                temp.add(header_temp);
            }
                candidates_list.add(temp);
        }
        List<List<String>> result= Lists.cartesianProduct(candidates_list).stream().filter(e->containsUnique(e)).distinct().collect(Collectors.toList());


        // convert to palmeto input
        int i = 0;
        String[][] candidate_array = new String[result.size()][];
        for (List<String> schema_candidate : result) {
            String[] schema_candidate_prep = schema_candidate.stream().toArray(String[]::new);
            candidate_array[i++] = schema_candidate_prep;
        }

        //caculate top k coherent candidate list
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


    private <T> boolean containsUnique(List<T> list) {
        return list.stream().allMatch(new HashSet<>()::add);
    }

}
