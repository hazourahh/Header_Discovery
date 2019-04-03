package de.uni_potsdam.hpi.table_header.io;

import java.io.File;

/**
 * @author Hazar Harmouch
 */
public final class Config {

    //folders
    public static String inputFolderPath = "data" + File.separator;
    public static String measurementsFolderPath, index_Folder;

    static {
        measurementsFolderPath = "io" + File.separator;
        index_Folder = measurementsFolderPath + "index" + File.separator;
    }

    //input
    public static final String FULL_WIKI_FILENAME = "tables.json";
    // public static final String FULL_WIKI_FILENAME = "table-small.json";

    public static final String TRAINING_WIKI_FILENAME = "tables_train.json";
    public static final String TESTING_WIKI_FILENAME = "table_test.json";
    public static final String Input_acsdb_file = "acsdb.txt";

    //results
    public enum Output {
        RESULT, SCHEMATA, NUMERC_NON, STATISTIC, WIDTH, LENGTH, NUMERIC, NON_NUMERIC, TABLES_MISSING_HEADERS, HEADERS, ACSDB, TABLEASCSV, TEST_SET, TRAIN_SET
    }

    public static final String ACSDB_FILENAME = Config.measurementsFolderPath + "acsdb";
    static final String Results_FILENAME;
    static final String HEADERS_FILENAME;
    public static final String HYPERTABLE_FILENAME = Config.measurementsFolderPath + "hypertables";
    static final String SCHEMATA_FILENAME;

    static {
        Results_FILENAME = Config.measurementsFolderPath + "result.csv";
        HEADERS_FILENAME = Config.measurementsFolderPath + "headers.csv";
        SCHEMATA_FILENAME = Config.measurementsFolderPath + "schemata.csv";
    }

    static final String NUMERC_NON_FILENAME = Config.measurementsFolderPath + "numeric_non.csv";
    static final String TABLES_MISSING_HEADERS_FILENAME = Config.measurementsFolderPath + "tables_missing_header";

    public static final String TABLEASCSV_Folder = "wiki_tables_csvs" + File.separator;

    //statistics
    static final String Statistics_FILENAME = Config.measurementsFolderPath + "stat.csv";// null headers or in header
    static final String WIDTH_FILENAME = Config.measurementsFolderPath + "width_stat.csv";
    static final String LENGTH_FILENAME = Config.measurementsFolderPath + "length_stat.csv";
    static final String NUMERC_FILENAME = Config.measurementsFolderPath + "numeric_stat.csv";
    static final String NON_NUMERC_FILENAME = Config.measurementsFolderPath + "non_numeric_stat.csv";


    //HLL length
    public static int HLLsize = 8;

    //Similarity Thresholds
    public static double table_similarity = 0.4;
    public static double column_similarity = 0.3;
    public static int number_tables = 1652771;

}
