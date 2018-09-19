package de.uni_potsdam.hpi.table_header.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Hazar Harmouch
 */
public class ResultWriter {

public static boolean NEW_RUN=true;
    public static void add2Result(String s, Config.Output type) {
        BufferedWriter bw = null;
        FileWriter fw = null;
        String out_file_name = "";

        File directory = new File(out_file_name);
        if (! directory.exists()) {
            directory.mkdir();
        }

        try {
            switch (type) {
                //result files
                case RESULT:
                    out_file_name = Config.Results_FILENAME;
                    break;
                case SCHEMATA:
                    out_file_name = Config.SCHEMATA_FILENAME;
                    break;
                case NUMERC_NON:
                    out_file_name = Config.NUMERC_NON_FILENAME;
                    break;
                case STATISTIC:
                    out_file_name = Config.Statistics_FILENAME;
                    break;
                case WIDTH:
                    out_file_name = Config.WIDTH_FILENAME;
                    break;
                case LENGTH:
                    out_file_name = Config.LENGTH_FILENAME;
                    break;
                case NUMERIC:
                    out_file_name = Config.NUMERC_FILENAME;
                    break;
                case NON_NUMERIC:
                    out_file_name = Config.NON_NUMERC_FILENAME;
                    break;
                case TABLES_MISSING_HEADERS:
                    out_file_name = Config.TABLES_MISSING_HEADERS_FILENAME;
                    break;
                case HEADERS:
                    out_file_name = Config.HEADERS_FILENAME;
                    break;
            }
            File file = new File(out_file_name);
            // if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }
            if(NEW_RUN)
            {NEW_RUN=false;
            fw = new FileWriter(file.getAbsoluteFile(), false);
            }
            else
            // true = append file
            fw = new FileWriter(file.getAbsoluteFile(), true);
            bw = new BufferedWriter(fw);

            bw.write(s);

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

    public static void add2Result(Map<Integer, Integer> items, Config.Output type)
    { StringBuilder result = new StringBuilder();
        for (Map.Entry<Integer, Integer> entry : items.entrySet()) {

            result.append(entry.getKey());
            result.append(",");
            result.append(entry.getValue());
            result.append("\r\n");

        }
        ResultWriter.add2Result(result.toString(), type);
    }
}


