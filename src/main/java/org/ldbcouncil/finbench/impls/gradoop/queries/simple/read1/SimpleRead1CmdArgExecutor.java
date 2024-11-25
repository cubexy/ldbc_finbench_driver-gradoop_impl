package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read1;

import java.util.Date;
import java.util.List;
import org.apache.logging.log4j.Logger;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1Result;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;

public class SimpleRead1CmdArgExecutor {
    public static void execute(FlinkCmdArg inputArgs, GradoopFinbenchBaseGraphState graph, Logger logger) throws DbException {
        logger.info("executing query SimpleRead1 with args {}", inputArgs);
        SimpleRead1 input = new SimpleRead1(inputArgs.getId(), inputArgs.getStartTime(), inputArgs.getEndTime());
        SimpleRead1GradoopOperator operator = new SimpleRead1GradoopOperator(input);
        logger.info("started execution...");
        List<SimpleRead1Result> result = operator.execute(graph.getGraph());
        logger.info("finished execution - result: {}", result);
    }
}
