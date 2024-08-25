package org.ldbcouncil.finbench.impls.gradoop;

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
import org.apache.flink.api.java.ExecutionEnvironment;

import java.io.IOException;
import java.util.Map;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1;
import org.ldbcouncil.finbench.impls.gradoop.queries.read.complex.*;
import org.ldbcouncil.finbench.impls.gradoop.queries.read.simple.*;

public class GradoopImpl extends Db {
    public static Logger logger = LogManager.getLogger("GradoopImpl");
    private TemporalGradoopConfig config;
    private GradoopFinbenchBaseGraphState graph;

    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        logger.info("Initializing Gradoop");

        final String gradoopGraphDataPath = properties.get("gradoop_import_path");
        final String mode = properties.get("gradoop_import_mode");

        if (gradoopGraphDataPath == null || mode == null || mode.isEmpty() || gradoopGraphDataPath.isEmpty()) {
            throw new DbException("gradoop_import_path or gradoop_import_mode not set in properties file");
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        this.config = TemporalGradoopConfig.createConfig(env);


        TemporalGraph tg = getTemporalGraph(mode, gradoopGraphDataPath);
        this.graph = new GradoopFinbenchBaseGraphState(tg);

        //complex reads go here
        registerOperationHandler(ComplexRead1.class, ComplexRead1Handler.class);
        //simple reads go here

    }

    private TemporalGraph getTemporalGraph(String mode, String gradoopGraphDataPath) throws DbException {
        final TemporalDataSource dataSource;
        switch (mode) {
            case "csv":
                dataSource = new TemporalCSVDataSource(gradoopGraphDataPath, this.config);
                break;
            case "indexed-csv":
                dataSource = new TemporalIndexedCSVDataSource(gradoopGraphDataPath, this.config);
                break;
            case "parquet":
                dataSource = new TemporalParquetDataSource(gradoopGraphDataPath, this.config);
                break;
            case "parquet-protobuf":
                dataSource = new TemporalParquetProtobufDataSource(gradoopGraphDataPath, this.config);
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
    protected void onClose() throws IOException {
        logger.info("Waiting for all tasks to finish...");
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return graph;
    }
}
