package org.ldbcouncil.finbench.impls.gradoop;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FlinkQueryExecutor {

    public static void main(String[] args) {
        Logger logger = LogManager.getLogger("FlinkGradoopQueryExecutor"); // initialize logger

        FlinkCmdArgParser parser = new FlinkCmdArgParser(args, logger); // initialize parser
        try {
            parser.parse(); // parse query arguments and initialize database
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
