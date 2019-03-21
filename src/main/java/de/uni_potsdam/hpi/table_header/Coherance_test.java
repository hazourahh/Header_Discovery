package de.uni_potsdam.hpi.table_header;

import de.uni_potsdam.hpi.table_header.io.Config;
import org.aksw.palmetto.Coherence;
import org.aksw.palmetto.DirectConfirmationBasedCoherence;
import org.aksw.palmetto.Palmetto;
import org.aksw.palmetto.aggregation.ArithmeticMean;
import org.aksw.palmetto.calculations.direct.*;
import org.aksw.palmetto.corpus.CorpusAdapter;
import org.aksw.palmetto.corpus.lucene.LuceneCorpusAdapter;
import org.aksw.palmetto.prob.bd.BooleanDocumentProbabilitySupplier;
import org.aksw.palmetto.subsets.OneAny;
import org.aksw.palmetto.subsets.OneOne;
import org.aksw.palmetto.subsets.OnePreceding;
import org.aksw.palmetto.subsets.OneSucceeding;

public class Coherance_test {

    public static void main(String[] args) {
        try {
            CorpusAdapter corpusAdapter = LuceneCorpusAdapter.create(Config.index_Folder, Palmetto.DEFAULT_TEXT_INDEX_FIELD_NAME);
            Coherence coherence_oneone_nlr = new DirectConfirmationBasedCoherence(new OneOne(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new NormalizedLogRatioConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_oneone_lr = new DirectConfirmationBasedCoherence(new OneOne(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new LogRatioConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_oneone_md = new DirectConfirmationBasedCoherence(new OneOne(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new DifferenceBasedConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_oneone_mf = new DirectConfirmationBasedCoherence(new OneOne(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new FitelsonConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_oneone_mc = new DirectConfirmationBasedCoherence(new OneOne(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new CondProbConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_oneone_mlj = new DirectConfirmationBasedCoherence(new OneOne(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new LogJaccardConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_oneone_mll = new DirectConfirmationBasedCoherence(new OneOne(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new LogLikelihoodConfirmationMeasure(), new ArithmeticMean());


            Coherence coherence_oneany_nlr = new DirectConfirmationBasedCoherence(new OneAny(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new NormalizedLogRatioConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_oneany_lr = new DirectConfirmationBasedCoherence(new OneAny(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new LogRatioConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_oneany_md = new DirectConfirmationBasedCoherence(new OneAny(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new DifferenceBasedConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_oneany_mf = new DirectConfirmationBasedCoherence(new OneAny(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new FitelsonConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_oneany_mc = new DirectConfirmationBasedCoherence(new OneAny(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new CondProbConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_oneany_mlj = new DirectConfirmationBasedCoherence(new OneAny(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new LogJaccardConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_oneany_mll = new DirectConfirmationBasedCoherence(new OneAny(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new LogLikelihoodConfirmationMeasure(), new ArithmeticMean());

            Coherence coherence_onepre_nlr = new DirectConfirmationBasedCoherence(new OnePreceding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new NormalizedLogRatioConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_onepre_lr = new DirectConfirmationBasedCoherence(new OnePreceding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new LogRatioConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_onepre_md = new DirectConfirmationBasedCoherence(new OnePreceding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new DifferenceBasedConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_onepre_mf = new DirectConfirmationBasedCoherence(new OnePreceding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new FitelsonConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_onepre_mc = new DirectConfirmationBasedCoherence(new OnePreceding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new CondProbConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_onepre_mlj = new DirectConfirmationBasedCoherence(new OnePreceding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new LogJaccardConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_onepre_mll = new DirectConfirmationBasedCoherence(new OnePreceding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new LogLikelihoodConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_onepre_mlc = new DirectConfirmationBasedCoherence(new OnePreceding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new LogCondProbConfirmationMeasure(), new ArithmeticMean());

            Coherence coherence_onesuc_nlr = new DirectConfirmationBasedCoherence(new OneSucceeding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new NormalizedLogRatioConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_onesuc_lr = new DirectConfirmationBasedCoherence(new OneSucceeding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new LogRatioConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_onesuc_md = new DirectConfirmationBasedCoherence(new OneSucceeding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new DifferenceBasedConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_onesuc_mf = new DirectConfirmationBasedCoherence(new OneSucceeding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new FitelsonConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_onesuc_mc = new DirectConfirmationBasedCoherence(new OneSucceeding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new CondProbConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_onesuc_mlj = new DirectConfirmationBasedCoherence(new OneSucceeding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new LogJaccardConfirmationMeasure(), new ArithmeticMean());
            Coherence coherence_onesuc_mll = new DirectConfirmationBasedCoherence(new OneSucceeding(),
                    BooleanDocumentProbabilitySupplier.create(corpusAdapter, "bd", true),
                    new LogLikelihoodConfirmationMeasure(), new ArithmeticMean());
            String[][] test={
                    {"age","from", "given", "info", "status", "surname"},
                    {"first_place_votes", "points", "team"},
                    {"points", "team","first_place_votes"},
                    {"area","households","lone_parents_with_dependent_children", "lone_parents_with_dependent_children", "population"},
                    {"year", "title","role","note"},
                    {"accession_position","(aa)_signature","id"},
                    {"lrg","grass","roots","three","blue","tee_lrg","grass","roots","three","blue","tee*"},
                    {"date","added_recipe_reviews"}
            };
            //----------------------------------
            System.out.print("oneone_nlr,");
            double coherences[] = coherence_oneone_nlr.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\noneone_lr,");
            coherences = coherence_oneone_lr.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\noneone_md,");
            coherences= coherence_oneone_md.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\noneone_mf,");
            coherences = coherence_oneone_mf.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\noneone_mc,");
            coherences =  coherence_oneone_mc.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\noneone_mlj,");
            coherences =  coherence_oneone_mlj.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\noneone_mll,");
            coherences =  coherence_oneone_mll.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");

            System.out.print("\noneany_nlr,");
            coherences= coherence_oneany_nlr.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\noneany_lr,");
            coherences = coherence_oneany_lr.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\noneany_md,");
            coherences= coherence_oneany_md.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\noneany_mf,");
            coherences = coherence_oneany_mf.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\noneany_mc,");
            coherences =  coherence_oneany_mc.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\noneany_mlj,");
            coherences =  coherence_oneany_mlj.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\noneany_mll,");
            coherences =  coherence_oneany_mll.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");

            System.out.print("\nonepre_nlr,");
            coherences= coherence_onepre_nlr.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\nonepre_lr,");
            coherences = coherence_onepre_lr.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\nonepre_md,");
            coherences= coherence_onepre_md.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\nonepre_mf,");
            coherences = coherence_onepre_mf.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\nonepre_mc,");
            coherences =  coherence_onepre_mc.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\nonepre_mlj,");
            coherences =  coherence_onepre_mlj.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\nonepre_mll,");
            coherences =  coherence_onepre_mll.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\nonepre_mlc,");
            coherences =  coherence_onepre_mlc.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");

            System.out.print("\nonesuc_nlr,");
            coherences= coherence_onesuc_nlr.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\nonesuc_lr,");
            coherences = coherence_onesuc_lr.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\nonesuc_md,");
            coherences= coherence_onesuc_md.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\nonesuc_mf,");
            coherences = coherence_onesuc_mf.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\nonesuc_mc,");
            coherences =  coherence_onesuc_mc.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\nonesuc_mlj,");
            coherences =  coherence_onesuc_mlj.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
            System.out.print("\nonesuc_mll,");
            coherences =  coherence_onesuc_mll.calculateCoherences(test);
            for(int i=0;i<coherences.length;i++)
                System.out.print(coherences[i]+",");
        }catch (Exception e)
        {}
    }
}



