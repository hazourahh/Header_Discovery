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

    public static  String TRAINING_WIKI_FILENAME = "tables_train.json";
    public static final String SMALL_TEST_FILENAME="tables_test_small.json";
    public static  String TESTING_WIKI_FILENAME = "table_test.json";
   // public static final String TESTING_WIKI_FILENAME = SMALL_TEST_FILENAME;
    public static final String FILTERED_FILENAME="tables_filtered.json";

    public static final String Input_acsdb_file = "acsdb.txt";


    //results
    public enum Output {
       RUNTIME, SMALL_TEST_SET, RESULT,RESULT_PHASE1, SCHEMATA, NUMERC_NON, STATISTIC, WIDTH, LENGTH, NUMERIC, NON_NUMERIC, TABLES_MISSING_HEADERS, HEADERS, ACSDB, TABLEASCSV, TEST_SET, TRAIN_SET, FILTERED_SET
    }


    public enum testdata {WEBTABLE,OPENDATA}

    public static final String ACSDB_FILENAME = Config.measurementsFolderPath + "acsdb";
    static final String Results_FILENAME, Results_phase1_FILENAME;

    static final String HEADERS_FILENAME;
    public static final String HYPERTABLE_FILENAME = Config.measurementsFolderPath + "hypertables";
    static final String SCHEMATA_FILENAME;

    static {
        Results_FILENAME = Config.measurementsFolderPath + "result.csv";
        Results_phase1_FILENAME = Config.measurementsFolderPath + "result_phase1.csv";
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
    static final String RUNTIME_FILENAME= Config.measurementsFolderPath + "runtime.csv";

    //HLL length
    //public static int HLLsize = 8;
    public static int HLL_PLUS_P=14;
    public static int HLL_PLUS_SP=25;
    //experiments config
    public static  int k = 5; //to choose top K candidate for each header
    public static int m = 10; //to choose top m candidate for each schema
    public static double table_similarity = 0.4;  // jaccard similarity between tables is larger than this number if table_similarity_filtering is enabled
    public static double column_similarity = 0; // the weighted containemnt is larger than this if table_similarity_weighting and table_similarity_filtering are enabled
    public static int number_tables = 1652771;  // for sampling
    public static int number_tables_after_preprocessing = 1078694;  // for sampling (574077 tables) filtered out
    public static boolean table_similarity_weighting=true;    // weight the column similarity by table similarity if table_similarity_filtering is enabled
    public static boolean table_similarity_filtering=true;   //filter on table similarity
    public static double table_similarity_weight=1; // how much weight for the tables similarity in the weighted score
    public static testdata test_type=testdata.WEBTABLE;
    public static boolean only_phase_1=false;
}
