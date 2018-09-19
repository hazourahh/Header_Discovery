package de.uni_potsdam.hpi.table_header.data_structures.hyper_table;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import de.uni_potsdam.hpi.table_header.io.Config;
import de.uni_potsdam.hpi.table_header.io.ResultWriter;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Hazar.Harmouch
 * This class is an in-memory representaion of webtable
 */
public class HTable implements Serializable {

    private String Name;/* table caption or file name in case of csv input file */
    private ArrayList<Column> Columns = new ArrayList<>();/* Columns (Label and HLL) */
    private int HLLbitSize;/* size of columns hyperloglog representaion */



    /**
     * Constructor
     * @param name table caption
     * @param HLLsize Hyperloglog size
     */

    public HTable(String name, int HLLsize) {
        super();
        Name = name;
        HLLbitSize = HLLsize;
    }
    /**
     * Constructor
     * @param name table caption
     * @param columns Arraylist of Columns
     * @param HLLsize Hyperloglog size
     */
    public HTable(String name, List<String> columns, int HLLsize) {
        super();
        Name = name;
        HLLbitSize = HLLsize;
        for (String c: columns
             ) {
            Columns.add(new Column(c, HLLsize));
        }
    }

    /**
     *
     * @return Table Caption
     */
    public String getName() {
        return Name;
    }

    /**
     *
     * @return arraylist of Columns
     */
    public  ArrayList<String> getHeaders() {
        ArrayList<String> header=new ArrayList<>();
        for (Column c: Columns)
        {header.add(c.getLabel());}
        return header;
    }

    /**
     *
     * @param column_index: the column to be updated
     * @param value: the new value to be added
     */
    public void add2Column(int column_index, Object value) {
        Columns.get(column_index).addValue(value);
    }

    /**
     *
     * @return the number of columns
     */
    public int getNumberCols() {return Columns.size();}


    private float findTableOverlap(HTable t) {
        long LHS_dist, RHS_dist, union_dist;
        float overlap = 0;
        HyperLogLog LHS_union, RHS_union,union;
        LHS_union=new HyperLogLog(HLLbitSize);
        RHS_union=new HyperLogLog(HLLbitSize);
        union=new HyperLogLog(HLLbitSize);

        try {

         for (Column col:Columns)
         LHS_union.addAll(col.getValues());
         LHS_dist=LHS_union.cardinality();

        for (Column col:t.Columns)
          RHS_union.addAll(col.getValues());
          RHS_dist=RHS_union.cardinality();


          union.addAll(LHS_union);
          union.addAll(RHS_union);
          union_dist = union.cardinality();

                    if (LHS_dist > 0 && RHS_dist > 0)
                        overlap = (LHS_dist + RHS_dist - union_dist) / (float) union_dist;

                } catch (CardinalityMergeException e) {
                    e.printStackTrace();
                }


return overlap;

    }

    /**
     * remove a column of the specified index
     * @param column_index the index of the column to be removed
     */
    public void removeEmptyColumn(int column_index)
    {Columns.remove(column_index);}

    public void findColumnOverlap(HTable t, double threashould) {


        long LHS_dist, RHS_dist, union_dist;
        float overlap = 0, weighted_overlap = 0, table_overlap = 0;
        Column LHS, RHS;

        table_overlap = findTableOverlap(t);


        for (int i = 0; i < Columns.size(); i++) {
            for (int j = 0; j < t.Columns.size(); j++) {
                LHS = Columns.get(i);
                RHS = t.Columns.get(j);
                LHS_dist = LHS.cardinality();
                RHS_dist = RHS.cardinality();
                HyperLogLog union = new HyperLogLog(HLLbitSize);
                try {
                    union.addAll(LHS.getValues());
                    union.addAll(RHS.getValues());
                    union_dist = union.cardinality();
                    if (LHS_dist > 0 && RHS_dist > 0)
                        overlap = (LHS_dist + RHS_dist - union_dist) / (float) union_dist;

                } catch (CardinalityMergeException e) {
                    e.printStackTrace();
                }
                //caculate the overlap and add results
                weighted_overlap = overlap * table_overlap; //commint this to go back to pure jaccard
                try {
                    if (overlap > threashould &&
                            LHS.getLabel() != null &&
                            RHS.getLabel() != null &&
                            //this.Name !=NULL &&
                            //t.getName() !=NULL &&
                            !StringUtils.isBlank(LHS.getLabel()) &&
                            !StringUtils.isBlank(RHS.getLabel())) {
                        StringBuilder result = new StringBuilder();
                        result.append(this.Name.replace(",", "-"));
                        result.append(",");
                        result.append(LHS.getLabel().replace(",", "-"));
                        result.append(",");
                        result.append(t.getName().replace(",", "-"));
                        result.append(",");
                        result.append(RHS.getLabel().replace(",", "-"));
                        result.append(",");
                        result.append(overlap);
                        result.append(",");
                        result.append(weighted_overlap);
                        result.append("\r\n");
                        ResultWriter.add2Result(result.toString(), Config.Output.RESULT);
                    }

                } catch (Exception e) {
                    System.out.println("error in find table"+this.Name+"-"+t.getName());
                }
            }


        }
    }
}
