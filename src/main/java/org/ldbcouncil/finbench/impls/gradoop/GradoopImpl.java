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
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4;
import org.ldbcouncil.finbench.impls.gradoop.queries.complex.read1.ComplexRead1Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.read1.SimpleRead1Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.read2.SimpleRead2Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.read3.SimpleRead3Handler;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.read4.SimpleRead4Handler;

public class GradoopImpl extends Db {
    public static Logger logger = LogManager.getLogger("GradoopImpl");
    private GradoopFinbenchBaseGraphState graph;

    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        logger.info("Initializing Gradoop");

        final String gradoopGraphDataPath = properties.get("gradoop_import_path");
        final String mode = properties.get("gradoop_import_mode");
        final String execMode = properties.get("gradoop_cluster_execution");

        if (gradoopGraphDataPath == null || mode == null || execMode == null || mode.isEmpty() ||
            gradoopGraphDataPath.isEmpty() || execMode.isEmpty()) {
            throw new DbException(
                "gradoop_import_path, gradoop_import_mode or gradoop_execution_mode not set in properties file");
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        TemporalGradoopConfig config = TemporalGradoopConfig.createConfig(env);

        TemporalGraph tg = getTemporalGraph(mode, gradoopGraphDataPath, config);
        this.graph = new GradoopFinbenchBaseGraphState(tg, execMode.equals("true"));

        //complex reads go here
        registerOperationHandler(ComplexRead1.class, ComplexRead1Handler.class);
        //simple reads go here
        registerOperationHandler(SimpleRead1.class, SimpleRead1Handler.class);
        registerOperationHandler(SimpleRead2.class, SimpleRead2Handler.class);
        registerOperationHandler(SimpleRead3.class, SimpleRead3Handler.class);
        registerOperationHandler(SimpleRead4.class, SimpleRead4Handler.class);
    }

    protected static TemporalGraph getTemporalGraph(String mode, String gradoopGraphDataPath,
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

    @Override
    protected void onClose() {
        logger.info("Waiting for all tasks to finish...");
    }

    @Override
    protected DbConnectionState getConnectionState() {
        return graph;
    }
}
