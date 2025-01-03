package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read7;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7Result;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.queries.AbstractCmdArgExecutor;

public class ComplexRead7CmdArgExecutor extends AbstractCmdArgExecutor<List<ComplexRead7Result>> {

    private ComplexRead7GradoopOperator operator;

    @Override
    public String getQueryTitle() {
        return "ComplexRead7";
    }

    @Override
    public String getQueryKey() {
        return "complex_read_7";
    }

    @Override
    public List<ComplexRead7Result> executeQuery(GradoopFinbenchBaseGraphState graph) throws DbException {
        return operator.execute(graph.getGraph());
    }

    @Override
    public void init(FlinkCmdArg inputArgs) {
        ComplexRead7 input = new ComplexRead7(inputArgs.getId(), inputArgs.getThreshold(), inputArgs.getStartTime(),
            inputArgs.getEndTime(), inputArgs.getTruncationLimit(), inputArgs.getTruncationOrder());
        this.operator = new ComplexRead7GradoopOperator(input);
    }
}
