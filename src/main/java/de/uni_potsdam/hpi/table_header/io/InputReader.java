package de.uni_potsdam.hpi.table_header.io;

import de.uni_potsdam.hpi.table_header.data_structures.hyper_table.HTable;
import de.uni_potsdam.hpi.table_header.data_structures.statistics_db.ACSDb;
import de.uni_potsdam.hpi.table_header.data_structures.statistics_db.WT_Schema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InputReader {
//read file
  private static List<List<String>> read_File(String Filename, String seperator)
    {   List<List<String>> values=new ArrayList<>();
        Pattern pattern= Pattern.compile(seperator);
        try (Stream<String> lines = Files.lines(Paths.get(Filename))) {
            values = lines.map(line -> Arrays.asList(pattern.split(line))).collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return values;
    }

//read a web table file as a hypertable
    public static HTable read_WT_File(String Filename,String seperator)
    {
        List<List<String>> values=read_File(Filename,seperator);
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

    //parse ACSDB file into ACSDB representation
    public static ACSDb read_ACSDB_File(String Filename)
    {   ACSDb db=new ACSDb();
        //TODO: filter to reduce size
        try (Stream<String> lines = Files.lines(Paths.get(Filename))) {
        lines.forEach(line -> {
            int first_dash=line.indexOf('_');
            int last_equal=line.lastIndexOf('=');
            String type=line.substring(0,first_dash);
            String the_schema=line.substring(first_dash+1,last_equal-1);
            String freq=line.substring(last_equal+1).trim();
            db.addSchema(new WT_Schema(type,the_schema)
                    ,Integer.parseInt(freq));
                }

        );
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        return  db;
}

}
