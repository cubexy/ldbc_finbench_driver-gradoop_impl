package org.ldbcouncil.finbench.impls.gradoop;

import java.io.IOException;
import java.util.Map;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSource;
import org.gradoop.temporal.io.impl.csv.indexed.TemporalIndexedCSVDataSource;
import org.gradoop.temporal.io.impl.parquet.plain.TemporalParquetDataSource;
import org.gradoop.temporal.io.impl.parquet.protobuf.TemporalParquetProtobufDataSource;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.util.TemporalGradoopConfig;
import org.ldbcouncil.finbench.driver.Db;
import org.ldbcouncil.finbench.driver.DbConnectionState;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.log.LoggingService;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead10;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead8;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead9;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead6;
import org.ldbcouncil.finbench.impls.gradoop.queries.complex.read1.ComplexRead1Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.complex.read10.ComplexRead10Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.complex.read11.ComplexRead11Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.complex.read12.ComplexRead12Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.complex.read2.ComplexRead2Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.complex.read3.ComplexRead3Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.complex.read4.ComplexRead4Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.complex.read5.ComplexRead5Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.complex.read6.ComplexRead6Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.complex.read7.ComplexRead7Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.complex.read8.ComplexRead8Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.complex.read9.ComplexRead9Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.read1.SimpleRead1Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.read2.SimpleRead2Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.read3.SimpleRead3Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.read4.SimpleRead4Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.read5.SimpleRead5Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.read6.SimpleRead6Handler;

public class GradoopImpl extends Db {
    public static Logger logger = LogManager.getLogger("GradoopImpl");
    private GradoopFinbenchBaseGraphState graph;

    /**
     * Initializes the TemporalGraph based on the specified import mode.
     *
     * @param mode                 import mode (csv | indexed-csv | parquet | parquet-protobuf)
     * @param gradoopGraphDataPath path to the Gradoop data
     * @param config               TemporalGradoopConfig
     * @return TemporalGraph
     * @throws DbException error while initializing the TemporalGraph
     */
    public static TemporalGraph getTemporalGraph(String mode, String gradoopGraphDataPath,
                                                 TemporalGradoopConfig config) throws DbException {
        final TemporalDataSource dataSource;
        switch (mode) {
            case "csv":
                dataSource = new TemporalCSVDataSource(gradoopGraphDataPath, config);
                break;
            case "indexed-csv":
                dataSource = new TemporalIndexedCSVDataSource(gradoopGraphDataPath, config);
                break;
            case "parquet":
                dataSource = new TemporalParquetDataSource(gradoopGraphDataPath, config);
                break;
            case "parquet-protobuf":
                dataSource = new TemporalParquetProtobufDataSource(gradoopGraphDataPath, config);
                break;
            default:
                throw new DbException("Unsupported import mode: " + mode);
        }

        TemporalGraph tg;
        try {
            tg = dataSource.getTemporalGraph();
        } catch (IOException e) {
            throw new DbException("Failed to load data from " + gradoopGraphDataPath, e);
        }
        return tg;
    }

    /**
     * Initializes the Gradoop implementation and registers all the available operations.
     *
     * @param properties     Input properties
     * @param loggingService Logging service
     * @throws DbException Error while initializing the Gradoop implementation
     */
    @Override
    public void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        logger.info("Initializing Gradoop");

        final String gradoopGraphDataPath = properties.get("gradoop_import_path");
        final String mode = properties.get("gradoop_import_mode");
        final boolean useFlinkSort = Boolean.parseBoolean(properties.get("use_flink_sort"));
        final int parallelism = Integer.parseInt(properties.get("parallelism"));

        if (gradoopGraphDataPath == null || mode == null || mode.isEmpty() ||
            gradoopGraphDataPath.isEmpty()) {
            throw new DbException(
                "gradoop_import_path, gradoop_import_mode or gradoop_execution_mode not set in properties file");
        }

        ExecutionEnvironment env =
            ExecutionEnvironment.createRemoteEnvironment("localhost", 8081, parallelism,
                "target/driver-0.2.0-alpha.jar"); // Set initial parallelism to 1 to allow the graph to load correctly

        TemporalGradoopConfig config = TemporalGradoopConfig.createConfig(env);

        TemporalGraph tg = getTemporalGraph(mode, gradoopGraphDataPath, config);
        this.graph = new GradoopFinbenchBaseGraphState(tg, useFlinkSort, parallelism);

        //complex reads go here
        registerOperationHandler(ComplexRead1.class, ComplexRead1Handler.class);
        registerOperationHandler(ComplexRead2.class, ComplexRead2Handler.class);
        registerOperationHandler(ComplexRead3.class, ComplexRead3Handler.class);
        registerOperationHandler(ComplexRead4.class, ComplexRead4Handler.class);
        registerOperationHandler(ComplexRead5.class, ComplexRead5Handler.class);
        registerOperationHandler(ComplexRead6.class, ComplexRead6Handler.class);
        registerOperationHandler(ComplexRead7.class, ComplexRead7Handler.class);
        registerOperationHandler(ComplexRead8.class, ComplexRead8Handler.class);
        registerOperationHandler(ComplexRead9.class, ComplexRead9Handler.class);
        registerOperationHandler(ComplexRead10.class, ComplexRead10Handler.class);
        registerOperationHandler(ComplexRead11.class, ComplexRead11Handler.class);
        registerOperationHandler(ComplexRead12.class, ComplexRead12Handler.class);

        //simple reads go here
        registerOperationHandler(SimpleRead1.class, SimpleRead1Handler.class);
        registerOperationHandler(SimpleRead2.class, SimpleRead2Handler.class);
        registerOperationHandler(SimpleRead3.class, SimpleRead3Handler.class);
        registerOperationHandler(SimpleRead4.class, SimpleRead4Handler.class);
        registerOperationHandler(SimpleRead5.class, SimpleRead5Handler.class);
        registerOperationHandler(SimpleRead6.class, SimpleRead6Handler.class);
        logger.info("Gradoop initialization complete");
    }

    /**
     * Closes the Gradoop implementation. (not used/needed)
     */
    @Override
    public void onClose() {
        logger.info("Waiting for all tasks to finish...");
    }

    /**
     * Returns the graph state.
     *
     * @return graph state
     */
    @Override
    public DbConnectionState getConnectionState() {
        return graph;
    }
}
