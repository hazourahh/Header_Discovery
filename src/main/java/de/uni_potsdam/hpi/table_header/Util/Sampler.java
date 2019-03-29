package de.uni_potsdam.hpi.table_header.Util;

import de.uni_potsdam.hpi.table_header.data_structures.wiki_table.WTable;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

public final class Sampler {
    public static Set<WTable> get_strtifiedSample_wiki_tables(Supplier<Stream<WTable>> tables, int sample_percent) {
        int Size_of_entire_population = 1652771;
        int max_length = 1523;
        int max_width = 64;
        int max_numeric = 42;
        int BIN_NUMBER = 3;
        float Size_of_entire_sample = sample_percent * Size_of_entire_population / 100;
        int sample_per_bin = Math.round(Size_of_entire_sample / (BIN_NUMBER * 3));
        Set<WTable> samples = new HashSet<>();

        float length_step = max_length / BIN_NUMBER;
        samples.addAll(get_strtifiedSample(tables, length_step, BIN_NUMBER, "LENGTH", sample_per_bin));
        float width_step = max_width / BIN_NUMBER;
        samples.addAll(get_strtifiedSample(tables, width_step, BIN_NUMBER, "WIDTH", sample_per_bin));
        float numeric_step = max_numeric / BIN_NUMBER;
        samples.addAll(get_strtifiedSample(tables, numeric_step, BIN_NUMBER, "NUMERIC", sample_per_bin));

        return samples;
    }

    public static Set<WTable> get_strtifiedSample(Supplier<Stream<WTable>> tables, float step, int bins, String type, int sample_per_bin) {
        Set<WTable> samples = new HashSet<WTable>();

        for (int i = 0; i <= bins; i++) {
            float start = i * step;
            float end = (i + 1) * step;
            Stream<WTable> strata = null;

            if (type.equals("WIDTH")) {
                strata = tables.get().filter(e -> !e.has_missing_header_line() && e.getNumCols() >= start && e.getNumCols() < end);
            } else if (type.equals("LENGTH")) {
                strata = tables.get().filter(e -> !e.has_missing_header_line() && e.getNumDataRows() >= start && e.getNumDataRows() < end);
            } else {
                strata = tables.get().filter(e -> !e.has_missing_header_line() && e.getNumericColumns().length >= start && e.getNumericColumns().length < end);
            }

            //int sample_size= Math.round((strata.size()/Size_of_entire_population)*Size_of_entire_sample);
            //samples.addAll(get_ReservoirSample(strata, sample_per_bin));
        }
        return samples;
    }

    public static HashSet<String> get_ReservoirSample(Stream<String> tables, int sampleSize) {
        HashSet<String> result = new HashSet<>(tables
                .collect(new ReservoirSampler<>(sampleSize)));
        // result.forEach(System.out::println);
        return result;

    }

}
