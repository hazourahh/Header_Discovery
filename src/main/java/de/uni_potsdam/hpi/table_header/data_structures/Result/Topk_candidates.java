package de.uni_potsdam.hpi.table_header.data_structures.Result;

import com.google.common.collect.MinMaxPriorityQueue;

import java.util.Arrays;
import java.util.Comparator;

/**
 * @author Hazar Harmouch
 */

public class Topk_candidates {
    private MinMaxPriorityQueue<Candidate>[] candidates;

    public Topk_candidates(int k, int numcol) {

        candidates = new MinMaxPriorityQueue[numcol];


        Comparator<Candidate> similarityComparator = (Candidate can1, Candidate can2) -> (Double.compare(can1.getSimilarity_score(), can2.getSimilarity_score()));

        for (int i = 0; i < candidates.length; i++) {
            candidates[i] = MinMaxPriorityQueue.orderedBy(similarityComparator.reversed())
                    .maximumSize(k)
                    .create();
        }
    }

    public MinMaxPriorityQueue<Candidate>[] getScored_candidates() {
        return candidates;
    }

    public void add_candidate(int col_index, Candidate can) {
        Candidate current = candidates[col_index].stream().filter(x -> x.equals(can)).findFirst().orElse(null);
        if (current != null) {
            candidates[col_index].remove(current);
            can.setSimilarity_score(Math.max(can.getSimilarity_score(), current.getSimilarity_score()));
        }
        candidates[col_index].add(can);

    }

    public void print() {
        System.out.println(Arrays.toString(candidates));

    }
}
