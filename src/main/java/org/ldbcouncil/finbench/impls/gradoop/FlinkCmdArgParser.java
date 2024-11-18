package org.ldbcouncil.finbench.impls.gradoop;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.parseUnixTimeString;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class FlinkCmdArgParser {
    private String mode;
    private String queryName;
    private long id;
    private long id2;
    private long personId;
    private long personId2;
    private Date startTime;
    private Date endTime;
    private int threshold;
    private int threshold2;
    private int truncationLimit;
    private int truncationOrder;
    private final String[] args;
    private CommandLine cmd;
    private final Options options;
    private final List<String> queries = Arrays.asList("simple_read_1", "simple_read_2", "simple_read_3", "simple_read_4", "simple_read_5",
        "simple_read_6", "complex_read_1", "complex_read_2", "complex_read_3", "complex_read_4", "complex_read_5",
        "complex_read_6", "complex_read_7", "complex_read_8", "complex_read_9", "complex_read_10",
        "complex_read_11", "complex_read_12");

    protected FlinkCmdArgParser(String[] args) {
        this.args = args;
        this.options = initCLIOptions();
    }

    protected void init() {
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        try {
            this.cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("FlinkQueryExecutor", this.options);
            System.exit(1);
        }
    }

    protected void parse() {
        final String queryName = this.cmd.getOptionValue("q");
        if (queryName == null) {
            throw new RuntimeException("Query name not set");
        }
        if (!this.queries.contains(queryName)) {
            throw new RuntimeException("Query " + queryName + " not found");
        }
        try {
            loadArgs();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // TODO: execute specific query

    }

    private void loadArgs() throws java.text.ParseException {
        if (this.cmd.hasOption("q_id")) {
            this.id = Long.parseLong(this.cmd.getOptionValue("q_id"));
        }
        if (this.cmd.hasOption("q_id2")) {
            this.id2 = Long.parseLong(this.cmd.getOptionValue("q_id2"));
        }
        if (this.cmd.hasOption("q_pid1")) {
            this.personId = Long.parseLong(this.cmd.getOptionValue("q_pid1"));
        }
        if (this.cmd.hasOption("q_pid2")) {
            this.personId2 = Long.parseLong(this.cmd.getOptionValue("q_pid2"));
        }
        if (this.cmd.hasOption("q_st")) {
            this.startTime = parseUnixTimeString(this.cmd.getOptionValue("q_st"));
        }
        if (this.cmd.hasOption("q_et")) {
            this.endTime = parseUnixTimeString(this.cmd.getOptionValue("q_et"));
        }
        if (this.cmd.hasOption("q_ts")) {
            this.threshold = Integer.parseInt(this.cmd.getOptionValue("q_ts"));
        }
        if (this.cmd.hasOption("q_ts2")) {
            this.threshold2 = Integer.parseInt(this.cmd.getOptionValue("q_ts2"));
        }
        if (this.cmd.hasOption("q_tl")) {
            this.truncationLimit = Integer.parseInt(this.cmd.getOptionValue("q_tl"));
        }
        if (this.cmd.hasOption("q_to")) {
            this.truncationOrder = Integer.parseInt(this.cmd.getOptionValue("q_to"));
        }
    }

    private static Options initCLIOptions() {
        Options options = new Options();
        options.addOption("q", "query", true, "Query to execute (simple_read_1..6 | complex_read_1..12)");
        options.addOption("m", "mode", true, "Import mode to use (csv | indexed-csv | parquet | parquet-protobuf)");
        options.addOption("q_id", "id", true, "[query arg] ID");
        options.addOption("q_id2", "id2", true, "[query arg] ID 2");
        options.addOption("q_pid1", "p_id_1", true, "[query arg] person ID 1");
        options.addOption("q_pid2", "p_id_2", true, "[query arg] person ID 2");
        options.addOption("q_st", "start_time", true, "[query arg] start time");
        options.addOption("q_et", "end_time", true, "[query arg] end time");
        options.addOption("q_ts", "threshold", true, "[query arg] threshold");
        options.addOption("q_ts2", "threshold_2", true, "[query arg] threshold 2");
        options.addOption("q_tl", "truncation_limit", true, "[query arg] truncation limit");
        options.addOption("q_to", "truncation_order", true, "[query arg] truncation order");
        return options;
    }

    private void initDatabase() {
        return;
    }
}
