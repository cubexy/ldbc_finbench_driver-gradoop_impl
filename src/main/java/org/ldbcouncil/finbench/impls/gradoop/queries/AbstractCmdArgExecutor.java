package org.ldbcouncil.finbench.impls.gradoop.queries;

import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;

public abstract class AbstractCmdArgExecutor<R> implements CmdArgExecutor<R> {

    @Override
    public R execute(FlinkCmdArg inputArgs, GradoopFinbenchBaseGraphState graph, Logger logger) throws DbException {
        logger.info("executing query {} with args {}", getQueryTitle(), inputArgs);
        init(inputArgs);
        logger.info("started execution...");
        R result = executeQuery(graph);
        logger.info("finished execution - result: {}", result);
        return result;
    }

    /**
     * Executes the query
     *
     * @param graph Input graph
     * @return query result
     * @throws DbException error while executing query
     */
    public abstract R executeQuery(GradoopFinbenchBaseGraphState graph) throws DbException;

    /**
     * Initializes the query
     *
     * @param inputArgs input arguments
     */
    public abstract void init(FlinkCmdArg inputArgs);

}
