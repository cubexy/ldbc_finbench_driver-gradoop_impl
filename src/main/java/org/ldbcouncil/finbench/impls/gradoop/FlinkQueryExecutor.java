package org.ldbcouncil.finbench.impls.gradoop;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.DbException;

public class FlinkQueryExecutor {

    public static void main(String[] args) {
        Logger logger = LogManager.getLogger("FlinkGradoopQueryExecutor"); // initialize logger

        FlinkCmdArgParser parser = new FlinkCmdArgParser(args, logger); // initialize parser
        try {
            parser.parse(); // parse query arguments and initialize database
        } catch (DbException e) {
            throw new RuntimeException(e);
        }
    }
}
