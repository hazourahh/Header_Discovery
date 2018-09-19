package de.uni_potsdam.hpi.table_header.io;

import de.uni_potsdam.hpi.table_header.data_structures.hyper_table.Column;
import de.uni_potsdam.hpi.table_header.data_structures.hyper_table.HTable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InputReader {
    public static HTable readFile(String Filename,String seperator)
    {

        List<List<String>> values=new ArrayList<>();
        try (Stream<String> lines = Files.lines(Paths.get(Filename))) {
            values = lines.map(line -> Arrays.asList(line.split(seperator))).collect(Collectors.toList());

        } catch (IOException e) {
            e.printStackTrace();
        }

        int numcols=values.get(0).size();
        List<String> dumy_headers=new ArrayList<>();
        for (int j=0;j<numcols;j++)
            dumy_headers.add("Column_"+j);
        HTable  hyper_table=new HTable(Filename,dumy_headers,Config.HLLsize);
        values.forEach(value -> {
                    for(int i=0;i<numcols;i++)
                         {hyper_table.add2Column(i,value.get(i));}
                }

        );
       return  hyper_table;
    }
}
