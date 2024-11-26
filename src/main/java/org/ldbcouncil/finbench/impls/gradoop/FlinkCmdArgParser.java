package org.ldbcouncil.finbench.impls.gradoop;

import static org.ldbcouncil.finbench.impls.gradoop.GradoopImpl.getTemporalGraph;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.logging.log4j.Logger;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.util.TemporalGradoopConfig;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.impls.gradoop.queries.complex.read1.ComplexRead1CmdArgExecutor;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.CmdArgExecutor;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.CmdArgExecutorRegistry;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.read1.SimpleRead1CmdArgExecutor;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.read2.SimpleRead2CmdArgExecutor;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.read3.SimpleRead3CmdArgExecutor;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.read4.SimpleRead4CmdArgExecutor;

public class FlinkCmdArgParser {
    private final Logger logger;
    private final String[] args;
    private final Options options;
    private final CmdArgExecutorRegistry executorRegistry;
    private CommandLine cmd;

    /**
     * Parses the command line arguments and initializes the database.
     *
     * @param args   command line arguments
     * @param logger logger
     */
    public FlinkCmdArgParser(String[] args, Logger logger) {
        this.args = args;
        this.logger = logger;
        this.options = initCLIOptions();
        this.executorRegistry = initExecutorRegistry();
    }

    /**
     * Parses the command line arguments and initializes the database.
     *
     * @throws DbException error while initializing the database
     */
    public void parse() throws DbException {
        logger.info("Initializing FlinkCmdArgParser...");
        init();
        logger.info("Reading command line arguments...");
        final FlinkCmdArg inputArgs = new FlinkCmdArg(cmd, executorRegistry.getAllExecutors().keySet());
        logger.info("FlinkCmdArgParser initialized.");

        logger.info("Initializing temporal graph...");
        final GradoopFinbenchBaseGraphState graph = initDatabase(inputArgs.getDataPath(), inputArgs.getMode());
        logger.info("FlinkCmdArgParser graph initialized.");

        logger.info("Executing query...");
        executeQuery(inputArgs, graph);
        logger.info("FlinkCmdArgParser query executed.");
    }


    /**
     * Initializes the command line parser.
     */
    private void init() {
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

    /**
     * Parses the command line arguments.
     *
     * @return available input arguments
     */
    private Options initCLIOptions() {
        Options options = new Options();
        options.addOption("q", "query", true, "Query to execute (simple_read_1..6 | complex_read_1..12)");
        options.addOption("m", "mode", true, "Import mode to use (csv | indexed-csv | parquet | parquet-protobuf)");
        options.addOption("d", "data_path", true, "gradoop data path");
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

    /**
     * initializes the executor registry
     * 
     * @return registry
     */
    private CmdArgExecutorRegistry initExecutorRegistry() {
        CmdArgExecutorRegistry registry = new CmdArgExecutorRegistry();
        registry.registerCmdArgExecutor(new ComplexRead1CmdArgExecutor());
        registry.registerCmdArgExecutor(new SimpleRead1CmdArgExecutor());
        registry.registerCmdArgExecutor(new SimpleRead2CmdArgExecutor());
        registry.registerCmdArgExecutor(new SimpleRead3CmdArgExecutor());
        registry.registerCmdArgExecutor(new SimpleRead4CmdArgExecutor());
        return registry;
    }

    /**
     * Initializes the database.
     *
     * @param gradoopDataPath path to the Gradoop data
     * @param mode            import mode (csv | indexed-csv | parquet | parquet-protobuf)
     * @return GradoopFinbenchBaseGraphState
     * @throws DbException error while initializing the database
     */
    private GradoopFinbenchBaseGraphState initDatabase(String gradoopDataPath, String mode) throws DbException {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        TemporalGradoopConfig config = TemporalGradoopConfig.createConfig(env);

        TemporalGraph tg = getTemporalGraph(mode, gradoopDataPath, config);
        return new GradoopFinbenchBaseGraphState(tg,
            false); // this command is being executed inside a cluster -> don't call another cluster
    }

    /**
     * Executes the query.
     *
     * @param inputArgs input arguments
     * @param graph     database
     */
    private void executeQuery(FlinkCmdArg inputArgs, GradoopFinbenchBaseGraphState graph) throws DbException {
        try {
            CmdArgExecutor<?> executor = this.executorRegistry.getCmdArgExecutorByTitle(inputArgs.getQueryName());
            executor.execute(inputArgs, graph, logger);
        } catch (Exception e) {
            logger.error("Error executing query", e);
            throw new DbException(e);
        }
    }
}