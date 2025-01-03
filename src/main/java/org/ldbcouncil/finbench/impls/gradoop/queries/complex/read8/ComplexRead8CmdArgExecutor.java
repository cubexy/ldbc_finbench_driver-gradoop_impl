package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read8;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead8;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead8Result;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.queries.AbstractCmdArgExecutor;

public class ComplexRead8CmdArgExecutor extends AbstractCmdArgExecutor<List<ComplexRead8Result>> {

    private ComplexRead8GradoopOperator operator;

    @Override
    public String getQueryTitle() {
        return "ComplexRead8";
    }

    @Override
    public String getQueryKey() {
        return "complex_read_8";
    }

    @Override
    public List<ComplexRead8Result> executeQuery(GradoopFinbenchBaseGraphState graph) throws DbException {
        return operator.execute(graph.getGraph());
    }

    @Override
    public void init(FlinkCmdArg inputArgs) {
        ComplexRead8 input = new ComplexRead8(inputArgs.getId(), inputArgs.getThreshold(), inputArgs.getStartTime(),
            inputArgs.getEndTime(), inputArgs.getTruncationLimit(), inputArgs.getTruncationOrder());
        this.operator = new ComplexRead8GradoopOperator(input);
    }
}
