package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read6;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6Result;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.queries.AbstractCmdArgExecutor;
import org.ldbcouncil.finbench.impls.gradoop.queries.complex.read6.ComplexRead6GradoopOperator;

public class ComplexRead6CmdArgExecutor extends AbstractCmdArgExecutor<List<ComplexRead6Result>> {

    private ComplexRead6GradoopOperator operator;

    @Override
    public String getQueryTitle() {
        return "ComplexRead6";
    }

    @Override
    public String getQueryKey() {
        return "complex_read_6";
    }

    @Override
    public List<ComplexRead6Result> executeQuery(GradoopFinbenchBaseGraphState graph) throws DbException {
        return operator.execute(graph.getGraph());
    }

    @Override
    public void init(FlinkCmdArg inputArgs) {
        ComplexRead6 input = new ComplexRead6(inputArgs.getId(), inputArgs.getThreshold(), inputArgs.getThreshold2(), inputArgs.getStartTime(), inputArgs.getEndTime(), inputArgs.getTruncationLimit(), inputArgs.getTruncationOrder());
        this.operator = new ComplexRead6GradoopOperator(input);
    }
}
