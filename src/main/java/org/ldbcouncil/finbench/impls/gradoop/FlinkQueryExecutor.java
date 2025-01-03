package org.ldbcouncil.finbench.impls.gradoop;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FlinkQueryExecutor {

    /**
     * Executes a singular query, independent of the FinBench driver.
     *
     * @param args: queryName: name of the query to execute
     *              dataPath: path to the Gradoop data
     *              id: ID
     *              id2: ID2
     *              personId: ID of a person
     *              personId2: ID of a person2
     *              startTime: start time of the query
     *              endTime: end time of the query
     *              threshold: threshold of the query
     *              threshold2: threshold of the query
     *              truncationLimit: truncation limit of the query
     *              truncationOrder: truncation order of the query
     */
    public static void main(String[] args) {
        Logger logger = LogManager.getLogger("FlinkQueryExecutor");

        logger.log(org.apache.logging.log4j.Level.INFO, "Starting Flink query executor");

        FlinkCmdArgParser parser = new FlinkCmdArgParser(args, logger); // initialize parser
        try {
            parser.parse(); // parse query arguments and initialize database
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
