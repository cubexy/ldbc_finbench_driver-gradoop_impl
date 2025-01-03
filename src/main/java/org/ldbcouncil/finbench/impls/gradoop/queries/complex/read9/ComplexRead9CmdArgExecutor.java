package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read9;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead9;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead9Result;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.queries.AbstractCmdArgExecutor;

public class ComplexRead9CmdArgExecutor extends AbstractCmdArgExecutor<List<ComplexRead9Result>> {

    private ComplexRead9GradoopOperator operator;

    @Override
    public String getQueryTitle() {
        return "ComplexRead9";
    }

    @Override
    public String getQueryKey() {
        return "complex_read_9";
    }

    @Override
    public List<ComplexRead9Result> executeQuery(GradoopFinbenchBaseGraphState graph) throws DbException {
        return operator.execute(graph.getGraph());
    }

    @Override
    public void init(FlinkCmdArg inputArgs) {
        ComplexRead9 input = new ComplexRead9(inputArgs.getId(), inputArgs.getThreshold(), inputArgs.getStartTime(),
            inputArgs.getEndTime(), inputArgs.getTruncationLimit(), inputArgs.getTruncationOrder());
        this.operator = new ComplexRead9GradoopOperator(input);
    }
}
