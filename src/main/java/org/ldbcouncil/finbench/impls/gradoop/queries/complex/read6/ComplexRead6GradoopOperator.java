package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read6;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;

class ComplexRead6GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead6Result>> {

    private final long id;
    private final long startTime;
    private final long endTime;
    private final int truncationLimit;
    private final boolean isTruncationOrderAscending;
    private final double threshold1;
    private final double threshold2;
    private final boolean useFlinkSort;

    public ComplexRead6GradoopOperator(ComplexRead6 cr6, boolean useFlinkSort) {
        this.id = cr6.getId();
        this.startTime = cr6.getStartTime().getTime();
        this.endTime = cr6.getEndTime().getTime();
        this.truncationLimit = cr6.getTruncationLimit();
        final TruncationOrder truncationOrder = cr6.getTruncationOrder();
        this.isTruncationOrderAscending = truncationOrder == TruncationOrder.TIMESTAMP_ASCENDING;
        this.threshold1 = cr6.getThreshold1();
        this.threshold2 = cr6.getThreshold2();
        this.useFlinkSort = useFlinkSort;
    }

    /**
     * Given an account of type card and a specified time window between startTime and endTime, find all
     * the connected accounts (mid) via withdrawal (edge2) satisfying, (1) More than 3 transfer-ins (edge1)
     * from other accounts (src) whose amount exceeds threshold1. (2) The amount of withdrawal (edge2)
     * from mid to dstCard whose exceeds threshold2. Return the sum of transfer amount from src to mid,
     * the amount from mid to dstCard grouped by mid.
     *
     * @param temporalGraph input graph
     * @return sum of transfer amount from src to mid, the amount from mid to dstCard grouped by mid
     */
    @Override
    public List<ComplexRead6Result> execute(TemporalGraph temporalGraph) {
        final double threshold1Serialized = this.threshold1;

        // TODO: implement truncation strategy
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer", "withdraw"))
            .fromTo(this.startTime, this.endTime);

        DataSet<Tuple4<Long, Double, Double, Integer>> edges = windowedGraph.query(
                "MATCH (src1:Account)-[edge1:transfer]->(mid:Account)-[edge2:withdraw]->(dstCard:Account) WHERE dstCard.id = " +
                    this.id + "L AND dstCard.type = 'card' AND edge1.amount > " + this.threshold1)
            .toGraphCollection()
            .getGraphTransactions()
            .map(new MapFunction<GraphTransaction, Tuple4<Long, Double, Double, Integer>>() {
                @Override
                public Tuple4<Long, Double, Double, Integer> map(GraphTransaction graphTransaction) {
                    Map<String, GradoopId> m = CommonUtils.getVariableMapping(graphTransaction);

                    GradoopId midGradoopId = m.get("mid");

                    double edge1Amount = 0;
                    double edge2Amount = 0;

                    Set<EPGMEdge> edges = graphTransaction.getEdges();

                    for (EPGMEdge edge : edges) {
                        if (edge.getLabel().equals("transfer")) {
                            edge1Amount = edge.getPropertyValue("amount").getDouble();
                        } else if (edge.getLabel().equals("withdraw")) {
                            edge2Amount = edge.getPropertyValue("amount").getDouble();
                        }
                    }

                    boolean exceedsThreshold = edge1Amount > threshold1Serialized;

                    Long midId =
                        graphTransaction.getVertexById(midGradoopId).getPropertyValue("id").getLong();
                    return new Tuple4<>(midId, edge1Amount, edge2Amount, exceedsThreshold ? 1 : 0);
                }
            })
            .groupBy(0)
            .reduce(new ReduceFunction<Tuple4<Long, Double, Double, Integer>>() {
                @Override
                public Tuple4<Long, Double, Double, Integer> reduce(Tuple4<Long, Double, Double, Integer> t1,
                                                                    Tuple4<Long, Double, Double, Integer> t2) {
                    return new Tuple4<>(t1.f0, t1.f1 + t2.f1, t1.f2 + t2.f2, t1.f3 + t2.f3);
                }
            })
            .filter(new FilterFunction<Tuple4<Long, Double, Double, Integer>>() {
                @Override
                public boolean filter(Tuple4<Long, Double, Double, Integer> t) {
                    return t.f3 > 3;
                }
            });

        if (this.useFlinkSort) {
            windowedGraph.getConfig().getExecutionEnvironment().setParallelism(1);

            edges = edges
                .sortPartition(2, Order.DESCENDING)
                .sortPartition(0, Order.ASCENDING);
        }

        List<Tuple4<Long, Double, Double, Integer>> edgesList;
        try {
            edgesList = edges.collect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (!this.useFlinkSort) {
            edgesList.sort(Comparator
                .comparing((Tuple4<Long, Double, Double, Integer> t) -> t.f2, Comparator.reverseOrder())
                .thenComparing(t -> t.f0));
        }

        List<ComplexRead6Result> complexRead6Results = new ArrayList<>();
        for (Tuple4<Long, Double, Double, Integer> edge : edgesList) {
            complexRead6Results.add(
                new ComplexRead6Result(edge.f0, roundToDecimalPlaces(edge.f1, 3), roundToDecimalPlaces(edge.f2, 3)));
        }

        return complexRead6Results;
    }
}
