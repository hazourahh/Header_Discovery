package de.uni_potsdam.hpi.table_header.benchmark;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.text.similarity.JaroWinklerDistance;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class source_table {


    public static void main(String[] args) {
        Pattern pattern_line = Pattern.compile(",");
        Pattern pattern_schema = Pattern.compile("-");
        BufferedWriter bw = null;
        FileWriter fw = null;
        String out_file_name = "schema_from_same_table.csv";
        File file = new File(out_file_name);

        try {
            Stream<String> result_lines = Files.lines(Paths.get("headers_result.csv"));
            Stream<String> train_tables_lines = Files.lines(Paths.get("headers_wiki.csv"));
            // if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }
            fw = new FileWriter(file.getAbsoluteFile(), false);
            bw = new BufferedWriter(fw);

            ArrayList<HashSet> train_tables_set=new ArrayList<HashSet>();
            for (String table_line : train_tables_lines.map(String::toLowerCase)
                    .collect(Collectors.toList())) {
                List<String> columns_headers = Arrays.asList(pattern_line.split(table_line));
                columns_headers.replaceAll(String::trim);
                HashSet<String> column_header_set = new HashSet<>(columns_headers.stream().map(ee->ee.replace(" ","_")) .collect(Collectors.toList()));
                train_tables_set.add(column_header_set);
            }

            for (String result_line: result_lines.map(String::toLowerCase).collect(Collectors.toList()) ) {
                List<String> columns = Arrays.asList(pattern_schema.split(result_line));
                columns.replaceAll(String::trim);
                HashSet<String> header_set= new HashSet<>(columns);
                header_set.remove("noresult");
                boolean included=false;
                if(header_set.size()>1) {
                    for (HashSet<String> table : train_tables_set) {
                         if (table.containsAll(header_set)) {
                            included = true;
                            break;
                        }
                    }
                }
                bw.write(result_line+";"+included+"\n");
                System.out.println(result_line+";"+included+"\n");

            }

            }
        catch (IOException e) {
            e.printStackTrace();
        }finally {

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
}
