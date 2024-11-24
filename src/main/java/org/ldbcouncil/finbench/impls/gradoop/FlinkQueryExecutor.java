package org.ldbcouncil.finbench.impls.gradoop;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FlinkQueryExecutor {

    /**
     * Executes a query.
     * @param args:
     *            queryName: name of the query to execute
     *            dataPath: path to the Gradoop data
     *            id: ID of the person to execute the query
     *            id2: ID of the person to execute the query
     *            personId: ID of the person to execute the query
     *            personId2: ID of the person to execute the query
     *            startTime: start time of the query
     *            endTime: end time of the query
     *            threshold: threshold of the query
     *            threshold2: threshold of the query
     *            truncationLimit: truncation limit of the query
     *            truncationOrder: truncation order of the query
     */
    public static void main(String[] args) {
        Logger logger = LogManager.getLogger("FlinkQueryExecutor");

        FlinkCmdArgParser parser = new FlinkCmdArgParser(args, logger); // initialize parser
        try {
            parser.parse(); // parse query arguments and initialize database
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
