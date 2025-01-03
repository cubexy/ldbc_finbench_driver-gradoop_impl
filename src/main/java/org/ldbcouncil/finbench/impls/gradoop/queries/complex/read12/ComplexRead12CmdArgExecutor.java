package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read12;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12Result;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.queries.AbstractCmdArgExecutor;
import org.ldbcouncil.finbench.impls.gradoop.queries.complex.read12.ComplexRead12GradoopOperator;

public class ComplexRead12CmdArgExecutor extends AbstractCmdArgExecutor<List<ComplexRead12Result>> {

    private ComplexRead12GradoopOperator operator;

    @Override
    public String getQueryTitle() {
        return "ComplexRead12";
    }

    @Override
    public String getQueryKey() {
        return "complex_read_12";
    }

    @Override
    public List<ComplexRead12Result> executeQuery(GradoopFinbenchBaseGraphState graph) throws DbException {
        return operator.execute(graph.getGraph());
    }

    @Override
    public void init(FlinkCmdArg inputArgs) {
        ComplexRead12 input = new ComplexRead12(inputArgs.getId(), inputArgs.getStartTime(), inputArgs.getEndTime(), inputArgs.getTruncationLimit(), inputArgs.getTruncationOrder());
        this.operator = new ComplexRead12GradoopOperator(input);
    }
}
