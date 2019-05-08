package de.uni_potsdam.hpi.table_header.io;

import de.uni_potsdam.hpi.table_header.data_structures.wiki_table.WTable;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * @author Hazar Harmouch
 */
public class ResultWriter {

    private static boolean NEW_RUN = true;

    public static void add2Result(String s, Config.Output type, String dataset) {
        BufferedWriter bw = null;
        FileWriter fw = null;
        String out_file_name = "";

        File directory = new File(out_file_name);
        if (!directory.exists()) {
            directory.mkdir();
        }

        try {
            switch (type) {
                //result files
                case RESULT:
                    out_file_name = Config.Results_FILENAME.replace(".csv", "_" + dataset + ".csv");
                    break;
                case SCHEMATA:
                    out_file_name = Config.SCHEMATA_FILENAME.replace(".csv", "_" + dataset + ".csv");
                    break;
                case NUMERC_NON:
                    out_file_name = Config.NUMERC_NON_FILENAME.replace(".csv", "_" + dataset + ".csv");
                    break;
                case STATISTIC:
                    out_file_name = Config.Statistics_FILENAME.replace(".csv", "_" + dataset + ".csv");
                    break;
                case WIDTH:
                    out_file_name = Config.WIDTH_FILENAME.replace(".csv", "_" + dataset + ".csv");
                    break;
                case LENGTH:
                    out_file_name = Config.LENGTH_FILENAME.replace(".csv", "_" + dataset + ".csv");
                    break;
                case NUMERIC:
                    out_file_name = Config.NUMERC_FILENAME.replace(".csv", "_" + dataset + ".csv");
                    break;
                case NON_NUMERIC:
                    out_file_name = Config.NON_NUMERC_FILENAME.replace(".csv", "_" + dataset + ".csv");
                    break;
                case TABLES_MISSING_HEADERS:
                    out_file_name = Config.TABLES_MISSING_HEADERS_FILENAME.replace(".csv", "_" + dataset + ".csv");
                    break;
                case HEADERS:
                    out_file_name = Config.HEADERS_FILENAME.replace(".csv", "_" + dataset + ".csv");
                    break;
                case ACSDB:
                    out_file_name = Config.ACSDB_FILENAME.replace(".csv", "_" + dataset + ".csv");
                    break;
                case TEST_SET:
                    out_file_name = Config.TESTING_WIKI_FILENAME.replace(".csv", "_" + dataset + ".csv");
                    break;
                case TRAIN_SET:
                    out_file_name = Config.TRAINING_WIKI_FILENAME.replace(".csv", "_" + dataset + ".csv");
                    break;
                case FILTERED_SET:
                    out_file_name = Config.FILTERED_FILENAME.replace(".csv", "_" + dataset + ".csv");
                    break;
            }
            File file = new File(out_file_name);
            // if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }
            if (NEW_RUN) {
                NEW_RUN = false;
                fw = new FileWriter(file.getAbsoluteFile(), false);
            } else
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

    public static void add2Result(Map<Integer, Integer> items, Config.Output type, String dataset) {
        StringBuilder result = new StringBuilder();
        for (Map.Entry<Integer, Integer> entry : items.entrySet()) {

            result.append(entry.getKey());
            result.append(",");
            result.append(entry.getValue());
            result.append("\r\n");

        }
        ResultWriter.add2Result(result.toString(), type, dataset);
    }

    public static void writeJSON_Tables(Collection<WTable> tables, Config.Output type, String dataset) {
        tables.forEach(e -> ResultWriter.add2Result(e.convert2JSON() + "\n", type, dataset));

    }


}


