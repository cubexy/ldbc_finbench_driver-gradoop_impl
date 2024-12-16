package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read2;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead2Result;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.queries.AbstractCmdArgExecutor;

public class ComplexRead2CmdArgExecutor extends AbstractCmdArgExecutor<List<ComplexRead2Result>> {

    private ComplexRead2GradoopOperator operator;

    @Override
    public String getQueryTitle() {
        return "ComplexRead2";
    }

    @Override
    public String getQueryKey() {
        return "complex_read_2";
    }

    @Override
    public List<ComplexRead2Result> executeQuery(GradoopFinbenchBaseGraphState graph) throws DbException {
        return operator.execute(graph.getGraph());
    }

    @Override
    public void init(FlinkCmdArg inputArgs) {
        ComplexRead2 input = new ComplexRead2(inputArgs.getId(), inputArgs.getStartTime(), inputArgs.getEndTime(), inputArgs.getTruncationLimit(), inputArgs.getTruncationOrder());
        this.operator = new ComplexRead2GradoopOperator(input);
    }
}
