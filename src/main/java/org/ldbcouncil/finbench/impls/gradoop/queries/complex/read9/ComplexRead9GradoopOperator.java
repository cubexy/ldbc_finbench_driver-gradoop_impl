package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read9;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead9;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead9Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;

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

    /**
     * Given an account, a bound of transfer amount and a specified time window between startTime and
     * endTime, find the deposit and repay edge between the account and a loan, the transfers-in and transfers-
     * out. Return ratioRepay (sum of all the edge1 over sum of all the edge2), ratioDeposit (sum of edge1
     * over sum of edge4), ratioTransfer (sum of edge3 over sum of edge4). Return -1 for ratioRepay if there
     * is no edge2 found. Return -1 for ratioDeposit and ratioTransfer if there is no edge4 found.
     * Note: There may be multiple loans that the given account is related to.
     *
     * @param temporalGraph input graph
     * @return ratioRepay, ratioDeposit, ratioTransfer
     */
    @Override
    public List<ComplexRead9Result> execute(TemporalGraph temporalGraph) {

        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account", "Loan"), new LabelIsIn<>("transfer", "repay", "deposit"))
            .fromTo(this.startTime, this.endTime);

        DataSet<GraphTransaction> edge1gt = windowedGraph.query("MATCH (loan1:Loan)-[edge1:deposit]->(mid:Account)" +
                " WHERE mid.id = " + this.id + "L" +
                " AND edge1.amount > " + this.threshold)
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> edge2gt = windowedGraph.query("MATCH (mid:Account)-[edge2:repay]->(loan2:Loan)" +
                " WHERE mid.id = " + this.id + "L" +
                " AND edge2.amount > " + this.threshold)
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> edge3gt = windowedGraph.query("MATCH (up:Account)-[edge3:transfer]->(mid:Account)" +
                " WHERE mid.id = " + this.id + "L" +
                " AND edge3.amount > " + this.threshold)
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> edge4gt = windowedGraph.query("MATCH (mid:Account)-[edge4:transfer]->(down:Account)" +
                " WHERE mid.id = " + this.id + "L" +
                " AND edge4.amount > " + this.threshold)
            .toGraphCollection()
            .getGraphTransactions();

        // get all transfers seperately

        DataSet<Tuple4<Double, Double, Double, Double>> result =
            edge1gt.union(edge2gt).union(edge3gt).union(edge4gt).map(
                    new MapFunction<GraphTransaction, Tuple4<Double, Double, Double, Double>>() {
                        @Override
                        public Tuple4<Double, Double, Double, Double> map(GraphTransaction graphTransaction) {
                            Set<EPGMEdge> edges = graphTransaction.getEdges();

                            double edge1Amount = 0;
                            double edge2Amount = 0;
                            double edge3Amount = 0;
                            double edge4Amount = 0;

                            for (EPGMEdge edge : edges) {
                                double edgeAmount = edge.getPropertyValue("amount").getDouble();
                                if (edge.getLabel().equals("deposit")) {
                                    edge1Amount = edgeAmount;
                                    continue;
                                }
                                if (edge.getLabel().equals("repay")) {
                                    edge2Amount = edgeAmount;
                                    continue;
                                }
                                // edge type 3 and 4 are transfers - we can find out which one it is by looking at the source and target
                                Map<String, GradoopId> m = CommonUtils.getVariableMapping(graphTransaction);
                                GradoopId midGradoopId = m.get("mid");
                                if (edge.getTargetId().equals(midGradoopId)) {
                                    edge3Amount = edgeAmount;
                                    continue;
                                }
                                edge4Amount = edgeAmount;
                            }
                            return new Tuple4<>(edge1Amount, edge2Amount, edge3Amount, edge4Amount);
                        }
                    }
                )
                .reduce(new ReduceFunction<Tuple4<Double, Double, Double, Double>>() {
                    @Override
                    public Tuple4<Double, Double, Double, Double> reduce(Tuple4<Double, Double, Double, Double> t1,
                                                                         Tuple4<Double, Double, Double, Double> t2) {
                        return new Tuple4<>(t1.f0 + t2.f0, t1.f1 + t2.f1, t1.f2 + t2.f2, t1.f3 + t2.f3);
                    }
                });

        Tuple4<Double, Double, Double, Double> data;

        try {
            data = result.collect().get(0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        double ratioRepay = data.f1.equals(0.0) ? 1.0 : CommonUtils.roundToDecimalPlaces(data.f0 / data.f1, 3);
        double ratioDeposit = data.f3.equals(0.0) ? 1.0 : CommonUtils.roundToDecimalPlaces(data.f0 / data.f3, 3);
        double ratioTransfer = data.f3.equals(0.0) ? 1.0 : CommonUtils.roundToDecimalPlaces(data.f2 / data.f3, 3);

        return Collections.singletonList(new ComplexRead9Result((float) ratioRepay, (float) ratioDeposit,
            (float) ratioTransfer));
    }
}
