package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read9;

import java.util.Date;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead9;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead9;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead9Result;

class ComplexRead9GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead9Result>> {

    private final long id;
    private final long startTime;
    private final long endTime;
    private final int truncationLimit;
    private final boolean isTruncationOrderAscending;
    private final double threshold;

    public ComplexRead9GradoopOperator(ComplexRead9 cr9) {
        this.id = cr9.getId();
        this.startTime = cr9.getStartTime().getTime();
        this.endTime = cr9.getEndTime().getTime();
        this.truncationLimit = cr9.getTruncationLimit();
        final TruncationOrder truncationOrder = cr9.getTruncationOrder();
        this.isTruncationOrderAscending = truncationOrder == TruncationOrder.TIMESTAMP_ASCENDING;
        this.threshold = cr9.getThreshold();
    }

    @Override
    public List<ComplexRead9Result> execute(TemporalGraph temporalGraph) {
        final double thresholdSerializable = this.threshold;

        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account", "Loan"), new LabelIsIn<>("transfer", "repay", "deposit"))
            .fromTo(this.startTime, this.endTime);

        try {
            List<GraphTransaction> t = windowedGraph.query("MATCH (loan:Loan)-[edge1:deposit]->(mid:Account)-[edge2:repay]->(loan)," +
                " (up:Account)-[edge3:transfer]->(mid)-[edge4:transfer]->(down:Account)" +
                " WHERE mid.id = " + this.id + "L AND edge1.amount > " + this.threshold +
                " AND edge2.amount > " + this.threshold +
                " AND edge3.amount > " + this.threshold +
                " AND edge4.amount > " + this.threshold)
                .toGraphCollection()
                .getGraphTransactions()
                .collect();

            t = t;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return null;
    }
}
