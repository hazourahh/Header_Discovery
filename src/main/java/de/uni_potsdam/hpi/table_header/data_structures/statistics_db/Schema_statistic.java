package de.uni_potsdam.hpi.table_header.data_structures.statistics_db;



import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Schema_statistic implements Serializable {

    //BloomFilter<String> schema;
    ArrayList<String> schema_strings;
    int count;


    public Schema_statistic(String schema, int count) {

            List<String> temp = Arrays.stream(schema.split("_"))
                    .map(e -> e.replaceAll("[\\]\\[(){},.;:!?<>%\\-*]", " "))
                    .map(String::trim)
                    .map(String::toLowerCase)
                    .collect(Collectors.toList());
            schema_strings= (ArrayList<String>) temp;
              this.count = count;
    }

   /* public boolean hasHeader(String header) {
        return schema.mightContain(header);
    }*/
    public boolean containsHeader(String header) {
        return schema_strings.contains(header);
    }

    public int getCount() {
        return count;
    }

}
