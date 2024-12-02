package org.ldbcouncil.finbench.impls.gradoop.queries.simple;

import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;

public interface CmdArgExecutor<T> {
    /**
     * Executes the command with the given arguments, graph state, and logger.
     *
     * @param inputArgs the input arguments for the execution
     * @param graph     the graph state to operate on
     * @param logger    the logger to use for logging information
     * @throws DbException if execution fails
     */
    T execute(FlinkCmdArg inputArgs, GradoopFinbenchBaseGraphState graph, Logger logger) throws DbException;

    /**
     * Gets the title of the query associated with this executor.
     *
     * @return the query title
     */
    String getQueryTitle();

    /**
     * Gets the key of the query associated with this executor.
     *
     * @return the query key
     */
    String getQueryKey();

}