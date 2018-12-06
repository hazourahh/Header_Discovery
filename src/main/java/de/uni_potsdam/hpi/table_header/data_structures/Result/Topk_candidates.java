package de.uni_potsdam.hpi.table_header.data_structures.Result;

import com.google.common.collect.MinMaxPriorityQueue;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;


public class Topk_candidates {
   private MinMaxPriorityQueue[] candidates;


    public Topk_candidates(int k, int numcol) {
        candidates=new  MinMaxPriorityQueue[numcol];
        Comparator<Header_Candidate> similarityComparator = (Header_Candidate can1, Header_Candidate can2) -> (Double.compare(can1.getSimilarity_score(),can2.getSimilarity_score()));

        for ( int i=0;i<candidates.length;i++)
        {candidates[i]=MinMaxPriorityQueue.orderedBy(similarityComparator.reversed())
                .maximumSize(k)
                .create();
        }

    }

    public MinMaxPriorityQueue<Header_Candidate>[] getCandidates() {
        return candidates;
    }

    public MinMaxPriorityQueue[] getCandidatelists() {
        return candidates;
    }

    public void add_candidate(int col_index, Header_Candidate can)
    {
       candidates[col_index].add(can);
    }

    public Set<String>[] unique()
    {
        Set<String>[] candidate_lists=new Set[candidates.length];
        for ( int i=0;i<candidate_lists.length;i++)
        {//candidate_lists[i]=candidates[i].stream().map(Header_Candidate::getHeader).collect(Collectors.toSet());


        }



      return candidate_lists;}

    public void print()
    {
        System.out.println(Arrays.toString(candidates));

    }
}
