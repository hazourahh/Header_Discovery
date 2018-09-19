package de.uni_potsdam.hpi.table_header.io;

import java.io.File;

public class Config {

   //folders
    public static String inputFolderPath = "data" + File.separator ;
    public static String measurementsFolderPath = "io" + File.separator;

   //input
    public static final String INPUT_WIKI_FILENAME = "tables.json";


   //results
   public enum Output {RESULT,SCHEMATA,NUMERC_NON,STATISTIC,WIDTH,LENGTH,NUMERIC,NON_NUMERIC,TABLES_MISSING_HEADERS,HEADERS}

  public static final String Results_FILENAME = Config.measurementsFolderPath+"result.csv";
    public static final String HEADERS_FILENAME = Config.measurementsFolderPath+"headers.csv";
  public static final String HYPERTABLE_FILENAME = Config.measurementsFolderPath+"hypertables";
  public static final String SCHEMATA_FILENAME = Config.measurementsFolderPath+"schemata.csv";
  public static final String NUMERC_NON_FILENAME = Config.measurementsFolderPath+"numeric_non.csv";
  public static final String TABLES_MISSING_HEADERS_FILENAME = Config.measurementsFolderPath+"tables_missing_header";

  //statistics
    public static final String Statistics_FILENAME = Config.measurementsFolderPath+"stat.csv";// null headers or in header
    public static final String WIDTH_FILENAME = Config.measurementsFolderPath+"width_stat.csv";
    public static final String LENGTH_FILENAME = Config.measurementsFolderPath+"length_stat.csv";
    public static final String NUMERC_FILENAME = Config.measurementsFolderPath+"numeric_stat.csv";
    public static final String NON_NUMERC_FILENAME = Config.measurementsFolderPath+"non_numeric_stat.csv";


//HLL length
  public static int HLLsize=8;
}
