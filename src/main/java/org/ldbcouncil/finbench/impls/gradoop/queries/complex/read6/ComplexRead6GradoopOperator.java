package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read6;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
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

    public ComplexRead6GradoopOperator(ComplexRead6 cr6) {
        this.id = cr6.getId();
        this.startTime = cr6.getStartTime().getTime();
        this.endTime = cr6.getEndTime().getTime();
        this.truncationLimit = cr6.getTruncationLimit();
        final TruncationOrder truncationOrder = cr6.getTruncationOrder();
        this.isTruncationOrderAscending = truncationOrder == TruncationOrder.TIMESTAMP_ASCENDING;
        this.threshold1 = cr6.getThreshold1();
        this.threshold2 = cr6.getThreshold2();
    }

    @Override
    public List<ComplexRead6Result> execute(TemporalGraph temporalGraph) {
        final long id_serializable = this.id;

        // TODO: implement truncation strategy
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer", "withdraw"))
            .fromTo(this.startTime, this.endTime);

        DataSet<Tuple3<Long, Double, Double>> edges = windowedGraph.query(
            "MATCH (src1:Account)-[edge1:transfer]->(mid:Account)-[edge2:withdraw]->(dstCard:Account) WHERE dstCard.id = " +
                this.id + "L AND dstCard.type = 'card' AND edge1.amount > " + this.threshold1 + " AND edge2.amount > " + this.threshold2)
            .toGraphCollection()
            .getGraphTransactions()
            .map(new MapFunction<GraphTransaction, Tuple3<Long, Double, Double>>() {
                @Override
                public Tuple3<Long, Double, Double> map(GraphTransaction graphTransaction) throws Exception {
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

                    Long midId =
                        graphTransaction.getVertexById(midGradoopId).getPropertyValue("id").getLong();
                    return new Tuple3<>(midId, edge1Amount, edge2Amount);
                }
            })
            .groupBy(0)
            .reduce(new ReduceFunction<Tuple3<Long, Double, Double>>() {
                @Override
                public Tuple3<Long, Double, Double> reduce(Tuple3<Long, Double, Double> t1,
                                                           Tuple3<Long, Double, Double> t2) throws Exception {
                    return new Tuple3<>(t1.f0, t1.f1 + t2.f1, t1.f2 + t2.f2);
                }
            });

        windowedGraph.getConfig().getExecutionEnvironment().setParallelism(1);

        edges = edges
            .sortPartition(2, Order.DESCENDING)
            .sortPartition(0, Order.ASCENDING);

        List<Tuple3<Long, Double, Double>> edgesList = null;
        try {
            edgesList = edges.collect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<ComplexRead6Result> complexRead6Results = new ArrayList<>();
        for (Tuple3<Long, Double, Double> edge : edgesList) {
            complexRead6Results.add(new ComplexRead6Result(edge.f0, roundToDecimalPlaces(edge.f1, 3), roundToDecimalPlaces(edge.f2, 3)));
        }

        return complexRead6Results;
    }
}
