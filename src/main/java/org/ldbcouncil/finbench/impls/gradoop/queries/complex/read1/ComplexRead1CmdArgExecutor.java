package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read1;

import java.util.Date;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.read1.ComplexRead1GradoopOperator;

public class ComplexRead1CmdArgExecutor {
    public static void execute(FlinkCmdArg inputArgs, GradoopFinbenchBaseGraphState graph, Logger logger) throws DbException {
        logger.info("executing query ComplexRead1 with args {}", inputArgs);
        ComplexRead1 input = new ComplexRead1(inputArgs.getId(), inputArgs.getStartTime(), inputArgs.getEndTime(), inputArgs.getTruncationLimit(), inputArgs.getTruncationOrder());
        ComplexRead1GradoopOperator operator = new ComplexRead1GradoopOperator(input);
        logger.info("started execution...");
        DataSet<Tuple4<Long, Integer, Long, String>> result = operator.execute(graph.getGraph());
        logger.info("finished execution - result: {}", result);
    }
}
