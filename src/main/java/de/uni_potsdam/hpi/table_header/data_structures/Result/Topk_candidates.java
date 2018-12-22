package de.uni_potsdam.hpi.table_header.data_structures.Result;

import com.google.common.collect.MinMaxPriorityQueue;

import java.util.*;

public class Topk_candidates {
   private MinMaxPriorityQueue<Candidate>[] scored_candidates;
   //private Set<String>[]  unique_candidates;

    public Topk_candidates(int k, int numcol) {

        scored_candidates=new  MinMaxPriorityQueue[numcol];
        //unique_candidates= new HashSet[numcol];

        Comparator<Candidate> similarityComparator = (Candidate can1, Candidate can2) -> (Double.compare(can1.getSimilarity_score(),can2.getSimilarity_score()));

        for ( int i=0;i<scored_candidates.length;i++)
        {scored_candidates[i]=MinMaxPriorityQueue.orderedBy(similarityComparator.reversed())
                .maximumSize(k)
                .create();
        }
    }

    public MinMaxPriorityQueue<Candidate>[] getScored_candidates() {
        return scored_candidates;
    }

   // public Set<String>[] getUnique_candidates() { return unique_candidates; }

    public void add_candidate(int col_index, Candidate can)
    {
        scored_candidates[col_index].add(can);
    }

   /* public void unique()
    {
        for ( int i=0;i<scored_candidates.length;i++)
        {
            unique_candidates[i] =scored_candidates[i].stream().map(e->e.getHeader()).collect(Collectors.toSet());

        }
    }*/

    public void print()
    {
        System.out.println(Arrays.toString(scored_candidates));

    }
}
