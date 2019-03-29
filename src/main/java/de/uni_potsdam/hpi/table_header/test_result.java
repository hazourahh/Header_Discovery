package de.uni_potsdam.hpi.table_header;

import com.github.andrewoma.dexx.collection.ArrayList;
import de.uni_potsdam.hpi.table_header.io.Config;
import de.uni_potsdam.hpi.table_header.io.InputReader;
import org.apache.commons.lang.StringUtils;

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

public class test_result {

    public static void main(String[] args) {
        Pattern pattern_line = Pattern.compile(";");
        Pattern pattern_schema = Pattern.compile("-");
        BufferedWriter bw = null;
        FileWriter fw = null;
        String out_file_name = "result_pre_mc_counting.csv";
        File file = new File(out_file_name);

        try (Stream<String> lines = Files.lines(Paths.get("result_pre_mc.csv"))) {
            // if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }
            fw = new FileWriter(file.getAbsoluteFile(), false);
            bw = new BufferedWriter(fw);

            for (String line:  lines.collect(Collectors.toList()) )
                   { List<String> columns=Arrays.asList(pattern_line.split(line));
                   int counter=0;
                   if(!StringUtils.isBlank(columns.get(6)))
                   {
                       List<String> result=Arrays.asList(pattern_schema.split(columns.get(6)));
                       List<String> original=Arrays.asList(pattern_schema.split(columns.get(7)));
                       for (int i=0;i<result.size();i++) {
                           if(result.get(i).trim().equals((original.get(i).trim().toLowerCase())))
                               counter++;

                       }

                   }
                       bw.write(line+";"+counter+"\n");
                   }

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
