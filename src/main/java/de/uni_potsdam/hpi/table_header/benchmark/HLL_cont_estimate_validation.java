package de.uni_potsdam.hpi.table_header.benchmark;


import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import de.uni_potsdam.hpi.table_header.data_structures.hyper_table.Column;
import de.uni_potsdam.hpi.table_header.data_structures.hyper_table.HTable;
import de.uni_potsdam.hpi.table_header.data_structures.wiki_table.WTable;
import de.uni_potsdam.hpi.table_header.io.Config;
import de.uni_potsdam.hpi.table_header.io.InputReader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public class HLL_cont_estimate_validation {

    static String out_file_name = "jaccard_column_level.csv";
    static BufferedWriter bw = null;
    static FileWriter fw = null;

    public static void main(String[] args) {

        try{
            File file = new File(out_file_name);
            if (!file.exists())
                file.createNewFile();

            fw = new FileWriter(file.getAbsoluteFile(), false);
            bw = new BufferedWriter(fw);

            Stream<String> Tables_Supplier = InputReader.parse_wiki_tables_file(Config.TESTING_WIKI_FILENAME);
        Tables_Supplier.forEach(line_t1 -> {

            WTable t1 = WTable.fromString(line_t1);
            HTable ht1=t1.Convert2Hyper();
            if(t1.getNumCols()==ht1.getNumberCols()) {
                Stream<String> Tables_Supplier_2 = InputReader.parse_wiki_tables_file(Config.TESTING_WIKI_FILENAME);
                Tables_Supplier_2.forEach(line_t2 ->
                {
                    WTable t2 = WTable.fromString(line_t2);
                    HTable ht2 = t2.Convert2Hyper();
                    long t1_dist, t2_dist, union_dist;

                    for (int i = 0; i < ht1.getColumns().size(); i++) {
                        for (int j = 0; j < ht2.getColumns().size(); j++) {
                            double overlap = 0,exact=0;
                            Column  t1_col = ht1.getColumns().get(i);
                            Column t2_col = ht2.getColumns().get(j);
                            t1_dist = t1_col.cardinality();
                            t2_dist = t2_col.cardinality();
                            HyperLogLogPlus union = new HyperLogLogPlus(Config.HLL_PLUS_P, Config.HLL_PLUS_SP);
                            try {
                                union.addAll(t1_col.getValues());
                                union.addAll(t2_col.getValues());
                            }catch (Exception ex)
                            {}
                            union_dist = union.cardinality();

                            if (t1_dist > 0 && t2_dist > 0)
                                overlap = (t1_dist + t2_dist - union_dist) / (float) t1_dist;


                            Set<String> col1_val = t1.getColumnValues(i);
                            Set<String> col2_val = t2.getColumnValues(j);
                            for (String item1:col1_val) {
                                for (String item2:col2_val)
                                    if(item1.equals(item2))
                                        exact+=1;
                            }
                            addtofile(t1.get_id(), t2.get_id(), t1_col.getLabel(), t2_col.getLabel(),exact/(float)col1_val.size(),overlap);
                        }}
                });


            }

        });



    } catch (Exception exx) {
        exx.printStackTrace();
    } finally {

        try {
            if (bw != null)
                bw.close();

            if (fw != null)
                fw.close();

        } catch (IOException ex) {

            ex.printStackTrace();

        }
    }
}

    static  void addtofile(String t1_id,String t2_id, String col1, String col2, double exact, double estimate)
    {
              try{  synchronized(Coherance_test.class) {
                  bw.write(t1_id+ ";" +
                               t2_id+ ";" +
                          col1+ ";" +
                          col2+ ";" +
                          exact+ ";" +
                          estimate+
                          "\n");

                  System.out.print(".");
              }
            }catch (IOException excp)
            { excp.printStackTrace();}
        }



    private static Double calculateJaccardSimilarity(HashSet<String> left, HashSet<String> right) {
        Set<String> intersectionSet = new HashSet<String>();
        Set<String> unionSet = new HashSet<String>();
        boolean unionFilled = false;
        int leftLength = left.size();
        int rightLength = right.size();
        if (leftLength == 0 || rightLength == 0) {
            return 0d;
        }

        for (String left_current: left)  {
            unionSet.add(left_current);
            for (String right_current: right){
                if (!unionFilled) {
                    unionSet.add(right_current);
                }
                if (left_current.equals(right_current) ) {
                    intersectionSet.add(String.valueOf(left_current));
                }
            }
            unionFilled = true;
        }
        return Double.valueOf(intersectionSet.size()) / Double.valueOf(unionSet.size());
    }


    private static float findTableOverlap(HTable input, HTable webtable) {
        //TODO: try different metrics
        long LHS_dist, RHS_dist, union_dist;


        float overlap = 0;
        HyperLogLogPlus LHS_union, RHS_union, union;
        LHS_union = new HyperLogLogPlus(Config.HLL_PLUS_P,Config.HLL_PLUS_SP);
        RHS_union = new HyperLogLogPlus(Config.HLL_PLUS_P,Config.HLL_PLUS_SP);
        union = new HyperLogLogPlus(Config.HLL_PLUS_P,Config.HLL_PLUS_SP);


        try {

            for (Column col : webtable.getColumns()) {
                LHS_union.addAll(col.getValues());
                union.addAll(col.getValues());
            }
            //cardinality of bag of words of a webtable
            LHS_dist = LHS_union.cardinality();
            for (Column col : input.getColumns()) {
                RHS_union.addAll(col.getValues());
                union.addAll(col.getValues());
            }
            //cardinality of bag of words of a inputtable
            RHS_dist = RHS_union.cardinality();
            //cardinality of bag of words of the union
            union_dist = union.cardinality();


            //intersection cardinality according to Inclusion-exclusion principle
            if (LHS_dist > 0 && RHS_dist > 0)
                overlap = (LHS_dist + RHS_dist - union_dist) / (float) union_dist ;


        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }


        return overlap;

    }
    }
