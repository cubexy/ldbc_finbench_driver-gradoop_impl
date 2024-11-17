package org.ldbcouncil.finbench.impls.gradoop;

import java.util.Arrays;
import org.apache.commons.cli.*;

public class FlinkQueryExecutor {

    public static void main(String[] args) {
        Options options = getCLIOptions();
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("FlinkQueryExecutor", options);
            System.exit(1);
        }
        if (cmd.hasOption("h")) {
            formatter.printHelp("FlinkQueryExecutor", options);
            System.exit(0);
        }
    }

    private static Options getCLIOptions() {
        Options options = new Options();
        options.addOption("q", "query", true, "Query to execute (simple_read_1..6 | complex_read_1..12)");
        options.addOption("m", "mode", true, "Import mode to use (csv | indexed-csv | parquet | parquet-protobuf)");
        options.addOption("q_id", "q_identifier", true, "[query arg] ID");
        options.addOption("q_id2", "q_identifier2", true, "[query arg] ID 2");
        options.addOption("q_pid1", "q_person_identifier_1", true, "[query arg] person ID 1");
        options.addOption("q_pid2", "q_person_identifier_2", true, "[query arg] person ID 2");
        options.addOption("q_st", "q_start_time", true, "[query arg] start time");
        options.addOption("q_et", "q_end_time", true, "[query arg] end time");
        options.addOption("q_ts", "q_threshold", true, "[query arg] threshold");
        options.addOption("q_ts2", "q_threshold_2", true, "[query arg] threshold 2");
        options.addOption("q_tl", "q_truncation_limit", true, "[query arg] truncation limit");
        options.addOption("q_to", "q_truncation_order", true, "[query arg] truncation order");
        return options;
    }
}
