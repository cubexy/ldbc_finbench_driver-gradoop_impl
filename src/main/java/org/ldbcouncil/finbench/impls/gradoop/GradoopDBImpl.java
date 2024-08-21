package org.ldbcouncil.finbench.impls.gradoop;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.Db;
import org.ldbcouncil.finbench.driver.DbConnectionState;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.log.LoggingService;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.IOException;
import java.util.Map;

public class GradoopDBImpl extends Db {
    static Logger logger = LogManager.getLogger("GradoopDbImpl");

    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        logger.info("Initializing Gradoop DB");
        //complex reads go here

        //simple reads go here

    }

    @Override
    protected void onClose() throws IOException {
        logger.info("Closing Gradoop DB");
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return null;
    }
}
