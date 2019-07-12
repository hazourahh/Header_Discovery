package de.uni_potsdam.hpi.table_header.benchmark;

import com.github.andrewoma.dexx.collection.ArrayList;
import de.uni_potsdam.hpi.table_header.io.Config;
import de.uni_potsdam.hpi.table_header.io.InputReader;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.text.similarity.JaroWinklerDistance;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class matching_percentage_caculator {

// results has the following format
// csv with ; as seperator
// table_id; web_page; table_caption;#row; #columns; #numeric_columns; the order of the schema in the top k;  result_scheam; original_schema
//last two fields are - separated strings where each part is a header and they preserve the input table column order
// we here add an extra field contains a matching percentage between the result and original schema

    public static void main(String[] args) {
        Pattern pattern_line = Pattern.compile(";");
        Pattern pattern_schema = Pattern.compile("-");
        BufferedWriter bw = null;
        FileWriter fw = null;
        String out_file_name = "result_counting_exact_exp6.csv";
        File file = new File(out_file_name);

        try (Stream<String> lines = Files.lines(Paths.get("result_exp6.csv"))) {
            // if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }
            fw = new FileWriter(file.getAbsoluteFile(), false);
            bw = new BufferedWriter(fw);
            float counter2=0;

            for (String line:  lines.collect(Collectors.toList()) )
                   { List<String> columns=Arrays.asList(pattern_line.split(line));
                   float counter=0;
                   if(!StringUtils.isBlank(columns.get(7)) && !columns.get(7).matches("[-]+") )
                   {
                       List<String> result=Arrays.asList(pattern_schema.split(columns.get(7)));
                       List<String> original=Arrays.asList(pattern_schema.split(columns.get(8)));
                       JaroWinklerDistance distance=new JaroWinklerDistance();
                       if(original.size()==result.size()) counter2++;
                       for (int i=0;i<result.size();i++) {
                           try {
                               if (result.get(i).trim().equals((original.get(i).trim().toLowerCase()))) //TODO: change here for counting
                                   // if(distance.apply(result.get(i).trim(),original.get(i).trim().toLowerCase())>0.7)
                                   counter++;
                           }catch (Exception e)
                           {
                               System.out.println(line);
                           }

                       }
                       counter=counter/result.size();


                   }

                       bw.write(line+";"+counter+"\n");
                   }
            System.out.println(counter2);

        } catch (IOException e) {
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
