package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read8;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead8;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead8Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;

class ComplexRead8GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead8Result>> {

    private final long id;
    private final long startTime;
    private final long endTime;
    private final int truncationLimit;
    private final boolean isTruncationOrderAscending;
    private final double threshold;

    public ComplexRead8GradoopOperator(ComplexRead8 cr8) {
        this.id = cr8.getId();
        this.startTime = cr8.getStartTime().getTime();
        this.endTime = cr8.getEndTime().getTime();
        this.truncationLimit = cr8.getTruncationLimit();
        final TruncationOrder truncationOrder = cr8.getTruncationOrder();
        this.isTruncationOrderAscending = truncationOrder == TruncationOrder.TIMESTAMP_ASCENDING;
        this.threshold = cr8.getThreshold();
    }

    /**
     * Given a Loan and a specified time window between startTime and endTime, trace the fund transfer
     * or withdraw by at most 3 steps from the account the Loan deposits. Note that the transfer paths of
     * edge1, edge2, edge3 and edge4 are in a specific time range between startTime and endTime. Amount
     * of each transfers or withdrawals between the account and the upstream account should exceed a
     * specified threshold of the upstream transfer. Return all the accounts’ id in the downstream of loan
     * with the final ratio and distanceFromLoan.
     * Note: Upstream of an edge refers to the aggregated total amounts of all transfer-in edges of its
     * source Account.
     *
     * @param temporalGraph input graph
     * @return all the accounts’ id in the downstream of loan with the final ratio and distanceFromLoan
     */
    @Override
    public List<ComplexRead8Result> execute(TemporalGraph temporalGraph) {
        // TODO: implement truncation strategy
        final double thresholdSerializable = this.threshold;

        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account", "Loan"), new LabelIsIn<>("transfer", "withdraw", "deposit"))
            .fromTo(this.startTime, this.endTime);

        DataSet<GraphTransaction> gtxLength3 = windowedGraph
            .temporalQuery(
                "MATCH (loan:Loan)-[edge1:deposit]->(src:Account)-[edge2]->(dst:Account)-[edge3]->(dst2:Account)-[edge4]->(dst3:Account)" +
                    " WHERE loan.id = " + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> gtxLength2 = windowedGraph
            .temporalQuery(
                "MATCH (loan:Loan)-[edge1:deposit]->(src:Account)-[edge2]->(dst:Account)-[edge3]->(dst2:Account)" +
                    " WHERE loan.id = " + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> gtxLength1 = windowedGraph
            .temporalQuery(
                "MATCH (loan:Loan)-[edge1:deposit]->(src:Account)-[edge2]->(dst:Account)" +
                    " WHERE loan.id = " + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<Tuple4<Long, Float, Integer, Boolean>> result =
            gtxLength1.union(gtxLength2).union(gtxLength3)
                .map(new MapFunction<GraphTransaction, Tuple4<Long, Float, Integer, Boolean>>() {
                    @Override
                    public Tuple4<Long, Float, Integer, Boolean> map(GraphTransaction graphTransaction) {
                        Map<String, GradoopId> m = CommonUtils.getVariableMapping(graphTransaction);
                        Set<EPGMEdge> edges = graphTransaction.getEdges();
                        int minAccountDistance = edges.size() - 1;

                        GradoopId loanGradoopId = m.get("loan");

                        double loanAmount =
                            graphTransaction.getVertexById(loanGradoopId).getPropertyValue("loanAmount").getDouble();
                        double inflow = 0.0;
                        boolean valid = true;

                        for (EPGMEdge edge : edges) {
                            if (edge.getLabel().equals("deposit")) {
                                continue;
                            }
                            double amount = edge.getPropertyValue("amount").getDouble();

                            valid = amount > inflow * thresholdSerializable;

                            if (edge.getLabel().equals("transfer")) {
                                inflow += amount;
                            }
                        }

                        GradoopId lastDstGradoopId = minAccountDistance > 2
                            ? m.get("dst3")
                            : minAccountDistance > 1
                            ? m.get("dst2")
                            : m.get("dst");

                        long lastDstId =
                            graphTransaction.getVertexById(lastDstGradoopId).getPropertyValue("id").getLong();

                        float ratio = (float) (inflow / loanAmount);

                        return new Tuple4<>(lastDstId, roundToDecimalPlaces(ratio, 3), minAccountDistance, valid);
                    }
                })
                .filter(new FilterFunction<Tuple4<Long, Float, Integer, Boolean>>() {
                    @Override
                    public boolean filter(Tuple4<Long, Float, Integer, Boolean> tuple) {
                        return tuple.f3;
                    }
                });

        windowedGraph.getConfig().getExecutionEnvironment().setParallelism(1);

        result = result
            .sortPartition(2, Order.DESCENDING)
            .sortPartition(1, Order.DESCENDING)
            .sortPartition(0, Order.ASCENDING);

        List<ComplexRead8Result> complexRead8Results = new ArrayList<>();

        try {
            List<Tuple4<Long, Float, Integer, Boolean>> resultList = result.collect();

            for (Tuple4<Long, Float, Integer, Boolean> tuple : resultList) {
                complexRead8Results.add(new ComplexRead8Result(tuple.f0, tuple.f1, tuple.f2));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return complexRead8Results;
    }
}
