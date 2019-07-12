package de.uni_potsdam.hpi.table_header.benchmark;

import de.uni_potsdam.hpi.table_header.data_structures.wiki_table.WTable;
import de.uni_potsdam.hpi.table_header.io.Config;
import de.uni_potsdam.hpi.table_header.io.InputReader;
import org.aksw.palmetto.Coherence;
import org.aksw.palmetto.DirectConfirmationBasedCoherence;
import org.aksw.palmetto.Palmetto;
import org.aksw.palmetto.aggregation.ArithmeticMean;
import org.aksw.palmetto.calculations.direct.*;
import org.aksw.palmetto.corpus.CorpusAdapter;
import org.aksw.palmetto.corpus.lucene.LuceneCorpusAdapter;
import org.aksw.palmetto.prob.bd.BooleanDocumentProbabilitySupplier;
import org.aksw.palmetto.subsets.OneOne;
import org.aksw.palmetto.subsets.OnePreceding;
import org.aksw.palmetto.subsets.OneSucceeding;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

public class Coherance_test {



    static CorpusAdapter corpusAdapter;
    static Coherence coherence_oneone_mc,coherence_oneone_md,coherence_oneone_mf ,coherence_oneone_lr, coherence_oneone_nlr,
                     coherence_onepre_mc,coherence_onepre_md,coherence_onepre_mf ,coherence_onepre_lr, coherence_onepre_nlr,
                     coherence_onesuc_mc,coherence_onesuc_md,coherence_onesuc_mf ,coherence_onesuc_lr, coherence_onesuc_nlr;

    static String out_file_name = "coherenc_test.csv";
    static BufferedWriter bw = null;
    static FileWriter fw = null;
    public static void main(String[] args) {

        try {
            corpusAdapter = LuceneCorpusAdapter.create(Config.index_Folder, Palmetto.DEFAULT_TEXT_INDEX_FIELD_NAME);

             coherence_oneone_nlr = new DirectConfirmationBasedCoherence(new OneOne(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new NormalizedLogRatioConfirmationMeasure(), new ArithmeticMean());
             coherence_oneone_lr = new DirectConfirmationBasedCoherence(new OneOne(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new LogRatioConfirmationMeasure(), new ArithmeticMean());
             coherence_oneone_md = new DirectConfirmationBasedCoherence(new OneOne(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new DifferenceBasedConfirmationMeasure(), new ArithmeticMean());
             coherence_oneone_mf = new DirectConfirmationBasedCoherence(new OneOne(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new FitelsonConfirmationMeasure(), new ArithmeticMean());
             coherence_oneone_mc = new DirectConfirmationBasedCoherence(new OneOne(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new CondProbConfirmationMeasure(), new ArithmeticMean());



             coherence_onepre_nlr = new DirectConfirmationBasedCoherence(new OnePreceding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new NormalizedLogRatioConfirmationMeasure(), new ArithmeticMean());
             coherence_onepre_lr = new DirectConfirmationBasedCoherence(new OnePreceding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new LogRatioConfirmationMeasure(), new ArithmeticMean());
            coherence_onepre_md = new DirectConfirmationBasedCoherence(new OnePreceding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new DifferenceBasedConfirmationMeasure(), new ArithmeticMean());
             coherence_onepre_mf = new DirectConfirmationBasedCoherence(new OnePreceding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new FitelsonConfirmationMeasure(), new ArithmeticMean());
            coherence_onepre_mc = new DirectConfirmationBasedCoherence(new OnePreceding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new CondProbConfirmationMeasure(), new ArithmeticMean());

            coherence_onesuc_nlr = new DirectConfirmationBasedCoherence(new OneSucceeding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new NormalizedLogRatioConfirmationMeasure(), new ArithmeticMean());
           coherence_onesuc_lr = new DirectConfirmationBasedCoherence(new OneSucceeding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new LogRatioConfirmationMeasure(), new ArithmeticMean());
             coherence_onesuc_md = new DirectConfirmationBasedCoherence(new OneSucceeding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new DifferenceBasedConfirmationMeasure(), new ArithmeticMean());
             coherence_onesuc_mf = new DirectConfirmationBasedCoherence(new OneSucceeding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new FitelsonConfirmationMeasure(), new ArithmeticMean());
             coherence_onesuc_mc = new DirectConfirmationBasedCoherence(new OneSucceeding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new CondProbConfirmationMeasure(), new ArithmeticMean());


            File file = new File(out_file_name);
                if (!file.exists())
                    file.createNewFile();

                fw = new FileWriter(file.getAbsoluteFile(), false);
                bw = new BufferedWriter(fw);



            Stream<String> Tables_Supplier = InputReader.parse_wiki_tables_file(Config.FULL_WIKI_FILENAME);
            Tables_Supplier.parallel().forEach(line -> {

                WTable t = WTable.fromString(line);
                if (t.getNumHeaderRows() == 1 &&
                        !t.has_missing_header()) {
                    List<String> columns = t.getHeaders();

            addtofile(columns);
                }
            });


        } catch (Exception exx) {
            exx.printStackTrace();
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

   static  void addtofile(List<String> columns)
    { String[][] test = new String[1][];
        test[0] = columns.stream().map(e -> e.replaceAll("[\\]\\[(){},.;:!?<>%\\-*]", " "))
                .map(String::trim)
                .map(String::toLowerCase).toArray(String[]::new);
        if (columns.size() > 1 && columns.size() <11) {
            try {
                System.out.println(String.join("-", test[0]));
                double oneone_mc=coherence_oneone_mc.calculateCoherences(test)[0] ; //0-1
                double oneone_md   =coherence_oneone_md.calculateCoherences(test)[0] ;  //-0.036562919 to 0.999997318
                double   oneone_mf = coherence_oneone_mf.calculateCoherences(test)[0] ; //-1 to 1
                double   oneone_lr = coherence_oneone_lr.calculateCoherences(test)[0] ; // -17.948670854 to 12.828780835
                double   oneone_nlr = coherence_oneone_nlr.calculateCoherences(test)[0] ;//-0.649584059 to 1.000000058
                double    onepre_mc= coherence_onepre_mc.calculateCoherences(test)[0] ; //0 to 1
                double  onepre_md  =  coherence_onepre_md.calculateCoherences(test)[0] ; //-0.058205651 to 0.999997318
                double   onepre_mf =  coherence_onepre_mf.calculateCoherences(test)[0] ;//-1 to 1
                double   onepre_lr =  coherence_onepre_lr.calculateCoherences(test)[0]; //-17.948670854 to 12.828780835
                double   onepre_nlr =  coherence_onepre_nlr.calculateCoherences(test)[0]; //-0.649584059 to 1.000000058
                double    onesuc_mc=  coherence_onesuc_mc.calculateCoherences(test)[0];//0 to 1
                double    onesuc_md=  coherence_onesuc_md.calculateCoherences(test)[0]; //-0.058205651 to 0.999997318
                double   onesuc_mf = coherence_onesuc_mf.calculateCoherences(test)[0] ; //-1 to 1
                double    onesuc_lr= coherence_onesuc_lr.calculateCoherences(test)[0] ; // -17.948670854 to 12.828780835
                double   onesuc_nlr = coherence_onesuc_nlr.calculateCoherences(test)[0] ;// -0.649584059 to 1.000000058
                synchronized(Coherance_test.class) {
                    bw.write(String.join("-", test[0]) + ";" +
                            oneone_mc + ";" +
                            oneone_md + ";" +
                            oneone_mf + ";" +
                            oneone_lr + ";" +
                            oneone_nlr + ";" +
                            onepre_mc + ";" +
                            onepre_md + ";" +
                            onepre_mf + ";" +
                            onepre_lr + ";" +
                            onepre_nlr + ";" +
                            onesuc_mc + ";" +
                            onesuc_md + ";" +
                            onesuc_mf + ";" +
                            onesuc_lr + ";" +
                            onesuc_nlr + ";" +
                            "\n");
                }
                System.out.println(String.join("-", test[0])+"____DONE____");
            }catch (IOException excp)
            { excp.printStackTrace();}
        }}
}


