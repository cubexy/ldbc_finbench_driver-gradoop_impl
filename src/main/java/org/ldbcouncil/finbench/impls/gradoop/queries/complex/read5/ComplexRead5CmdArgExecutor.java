package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read5;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead5Result;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.queries.AbstractCmdArgExecutor;

public class ComplexRead5CmdArgExecutor extends AbstractCmdArgExecutor<List<ComplexRead5Result>> {

    private ComplexRead5GradoopOperator operator;

    @Override
    public String getQueryTitle() {
        return "ComplexRead5";
    }

    @Override
    public String getQueryKey() {
        return "complex_read_5";
    }

    @Override
    public List<ComplexRead5Result> executeQuery(GradoopFinbenchBaseGraphState graph) throws DbException {
        return operator.execute(graph.getGraph());
    }

    @Override
    public void init(FlinkCmdArg inputArgs) {
        ComplexRead5 input = new ComplexRead5(inputArgs.getId(), inputArgs.getStartTime(), inputArgs.getEndTime(),
            inputArgs.getTruncationLimit(), inputArgs.getTruncationOrder());
        this.operator = new ComplexRead5GradoopOperator(input);
    }
}
