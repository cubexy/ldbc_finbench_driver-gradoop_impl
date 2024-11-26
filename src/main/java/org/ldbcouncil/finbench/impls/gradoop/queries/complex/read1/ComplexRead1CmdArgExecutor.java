package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read1;

import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1Result;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.AbstractCmdArgExecutor;

public class ComplexRead1CmdArgExecutor extends AbstractCmdArgExecutor<List<ComplexRead1Result>> {

    private ComplexRead1GradoopOperator operator;

    @Override
    public String getQueryTitle() {
        return "ComplexRead1";
    }

    @Override
    public String getQueryKey() {
        return "complex_read_1";
    }

    @Override
    public List<ComplexRead1Result> executeQuery(GradoopFinbenchBaseGraphState graph) throws DbException {
        return operator.execute(graph.getGraph());
    }

    @Override
    public void init(FlinkCmdArg inputArgs) {
        ComplexRead1 input = new ComplexRead1(inputArgs.getId(), inputArgs.getStartTime(), inputArgs.getEndTime(), inputArgs.getTruncationLimit(), inputArgs.getTruncationOrder());
        this.operator = new ComplexRead1GradoopOperator(input);
    }
}
