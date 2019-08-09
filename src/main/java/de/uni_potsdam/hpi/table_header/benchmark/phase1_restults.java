package de.uni_potsdam.hpi.table_header.benchmark;

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

public class phase1_restults {
    // results has the following format

// csv with ; as seperator
// table_id; web_page; table_caption;#row; #columns; #numeric_columns; original header; header_candidates
//last  field is - separated strings where each part is a header candidaite and they preserve the similarity order
// we here add two extra fields match (true,false), match position

    public static void main(String[] args) {
        Pattern pattern_line = Pattern.compile(";");
        Pattern pattern_schema = Pattern.compile("-");
        BufferedWriter bw = null;
        FileWriter fw = null;
        String out_file_name = "result_phase1_top10_0.2_match.csv";
        File file = new File(out_file_name);

        try (Stream<String> lines = Files.lines(Paths.get("result_phase1_top10_0.2.csv"))) {
            // if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }
            fw = new FileWriter(file.getAbsoluteFile(), false);
            bw = new BufferedWriter(fw);
            float counter2 = 0;

            for (String line : lines.collect(Collectors.toList())) {
                List<String> columns = Arrays.asList(pattern_line.split(line));
                String match = "DIFF";
                int index = 0;
                if (!StringUtils.isBlank(columns.get(7)) && !columns.get(7).matches("[-]+")) {
                    List<String> result = Arrays.asList(pattern_schema.split(columns.get(7)));
                    String original = columns.get(6);
                    // JaroWinklerDistance distance = new JaroWinklerDistance();
                    if (result.size() == 1 && result.get(0).equals("NORESULT"))
                        match = "NORESULT";
                    else {
                        for (int i = 0; i < result.size(); i++) {
                            try {
                                if (result.get(i).trim().equals(original.trim().toLowerCase())) //TODO: change here for counting
                                // if(distance.apply(result.get(i).trim(),original.get(i).trim().toLowerCase())>0.7)
                                {
                                    match = "MATCH";
                                    index = i + 1;
                                }
                            } catch (Exception e) {
                                System.out.println(line);
                            }

                        }
                    }
                }

                bw.write(line + ";" + match + ";" + index + "\n");
            }
            System.out.println(counter2);

        } catch (IOException e) {
            e.printStackTrace();
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
}