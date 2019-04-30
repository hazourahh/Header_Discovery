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
import org.aksw.palmetto.aggregation.ArithmeticMean;
import org.aksw.palmetto.calculations.direct.CondProbConfirmationMeasure;
import org.aksw.palmetto.calculations.direct.DirectConfirmationMeasure;
import org.aksw.palmetto.corpus.CorpusAdapter;
import org.aksw.palmetto.corpus.lucene.LuceneCorpusAdapter;
import org.aksw.palmetto.prob.bd.BooleanDocumentProbabilitySupplier;
import org.aksw.palmetto.subsets.OnePreceding;
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

    private Coherence coherence;


    //filters??
    //TODO : filter long attribute of schemata happens once?

    /***
     *
     * @param segmentation
     * @param confirmation
     * @param aggregation
     */
   public  Coherent_Blinder(Segmentator segmentation, DirectConfirmationMeasure confirmation, Aggregation aggregation)
    { try {
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
    /***
     *   build inverted index and coherence measure to evaluate the coherence of the schema candidates

     */
   public static void initialize() {

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
    }

//----------------------------------------------------------------


    public Topk_candidates coherant_blind_candidate(Topk_candidates candidates, int k) {


        //to caculate top k coherent candidate list
        Topk_candidates schema_candidates = new Topk_candidates(k, 1);
        {
            List<List<String>> result; //lists to keep the order of top k headers
            {
                //build permutations and process all strings to have _ instead space to mach the index
                List<List<String>> candidates_list = new ArrayList<>();
                for (int i = 0; i < candidates.getScored_candidates().length; i++) {
                    List<String> temp = new ArrayList<>();
                    if (candidates.getScored_candidates()[i].isEmpty()) {
                        temp.add("NORESULT");
                        //candidates_list.add(temp);
                    }
                    for (Candidate j : candidates.getScored_candidates()[i]) {
                        String header_temp = ((Header_Candidate) j).getHeader().trim().toLowerCase().replaceAll(" ", "_");
                        temp.add(header_temp);
                    }
                    candidates_list.add(temp);
                }
                result = Lists.cartesianProduct(candidates_list).stream().filter(e -> containsUnique(e)).distinct().collect(Collectors.toList());
            }

            // convert to palmeto input
            //int i = 0;
            //candidate_array = new String[result.size()][];
            //CartesianIterable <String> ci = new CartesianIterable <String> (candidates_list);

            for (List<String> schema_candidate : result) {
                String[] schema_candidate_prep = schema_candidate.stream().filter(e->!e.equals("NORESULT")).toArray(String[]::new);
                //candidate_array[i++] = schema_candidate_prep;
                String[][] candidate_array = new String[1][];
                candidate_array[0] = schema_candidate_prep;
                double coherences[]={};
                if(schema_candidate_prep.length<2)
                    coherences[0]=0;
                else
                    coherences= coherence.calculateCoherences(candidate_array);
                if ((schema_candidate_prep != null) && (schema_candidate_prep.length > 0)) {

                    schema_candidates.add_candidate(0,
                            new Schema_Candidate(schema_candidate, coherences[0]));

            }
        }
//          double coherences[] = coherence.calculateCoherences(candidate_array);
//        int j = 0;
//        for (String[] schema_candidate : candidate_array) {
//            if ((schema_candidate != null) && (schema_candidate.length > 0)) {
//
//                schema_candidates.add_candidate(0,
//                        new Schema_Candidate(Arrays.asList(schema_candidate), coherences[j]));
//            }
//            j++;
        }
return schema_candidates;
    }


    private <T> boolean containsUnique(List<T> list) {
        return list.stream().filter(e->e!="NORESULT").allMatch(new HashSet<>()::add);
    }

}


class CartesianIterable <T> implements Iterable <List <T>> {

    private List <List <T>> lilio;

    public CartesianIterable (List <List <T>> llo) {
        lilio = llo;
    }

    public Iterator <List <T>> iterator () {
        return new CartesianIterator <T> (lilio);
    }
}

class CartesianIterator <T> implements Iterator <List <T>> {

    private final List <List <T>> lilio;
    private int current = 0;
    private final long last;

    public CartesianIterator (final List <List <T>> llo) {
        lilio = llo;
        long product = 1L;
        for (List <T> lio: lilio)
            product *= lio.size ();
        last = product;
    }

    public boolean hasNext () {
        return current != last;
    }

    public List <T> next () {
        ++current;
        return get (current - 1, lilio);
    }

    public void remove () {
        ++current;
    }

    private List<T> get (final int n, final List <List <T>> lili) {
        switch (lili.size ())
        {
            case 0: return new ArrayList <T> (); // no break past return;
            default: {
                List <T> inner = lili.get (0);
                List <T> lo = new ArrayList <T> ();
                lo.add (inner.get (n % inner.size ()));
                lo.addAll (get (n / inner.size (), lili.subList (1, lili.size ())));
                return lo;
            }
        }
    }
}
