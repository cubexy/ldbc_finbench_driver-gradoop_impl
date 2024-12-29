package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read7;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumProperty;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7Result;

class ComplexRead7GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead7Result>> {

    private final long id;
    private final long startTime;
    private final long endTime;
    private final int truncationLimit;
    private final boolean isTruncationOrderAscending;
    private final double threshold;

    public ComplexRead7GradoopOperator(ComplexRead7 cr7) {
        this.id = cr7.getId();
        this.startTime = cr7.getStartTime().getTime();
        this.endTime = cr7.getEndTime().getTime();
        this.truncationLimit = cr7.getTruncationLimit();
        final TruncationOrder truncationOrder = cr7.getTruncationOrder();
        this.isTruncationOrderAscending = truncationOrder == TruncationOrder.TIMESTAMP_ASCENDING;
        this.threshold = cr7.getThreshold();
    }

    @Override
    public List<ComplexRead7Result> execute(TemporalGraph temporalGraph) {
        final long id_serializable = this.id;

        // TODO: implement truncation strategy
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer", "withdraw"))
            .fromTo(this.startTime, this.endTime);

        TemporalGraph tg = windowedGraph.query(
                "MATCH (src:Account)-[edge1:transfer]->(mid:Account)-[edge2:transfer]->(dst:Account) WHERE mid.id = " +
                    this.id + "L AND dstCard.type = 'card' AND edge1.amount > " + this.threshold + " AND edge2.amount > " +
                    this.threshold)
            .reduce(new ReduceCombination<>())
            .transformVertices((currentVertex, transformedVertex) -> {
                if (currentVertex.hasProperty("id") &&
                    !Objects.equals(currentVertex.getPropertyValue("id").getLong(), id_serializable)) {
                    currentVertex.removeProperty("id");
                }
                return currentVertex;
            })
            .callForGraph(
                new KeyedGrouping<>(Arrays.asList(GroupingKeys.label(), GroupingKeys.property("id")),
                    null, null,
                    Arrays.asList(new Count("count"), new SumProperty("amount"))));

        MapOperator<Tuple2<Tuple3<Long, Integer, Double>, Tuple3<Long, Integer, Double>>, Tuple3<Integer, Integer, Double>>
            edgeValues = tg.getEdges()
            .join(tg.getVertices()).where(new SourceId<>()).equalTo(new Id<>())
            .map(new MapFunction<Tuple2<TemporalEdge, TemporalVertex>, Tuple3<Long, Integer, Double>>() {
                @Override
                public Tuple3<Long, Integer, Double> map(Tuple2<TemporalEdge, TemporalVertex> e)
                    throws Exception {
                    TemporalEdge edge2 = e.f0;
                    TemporalVertex src = e.f1;

                    long srcId = src.getPropertyValue("id").getLong();
                    int edge2Count = (int) edge2.getPropertyValue("count").getLong();
                    double edge2Sum = edge2.getPropertyValue("sum_amount").getDouble();

                    return new Tuple3<>(srcId, edge2Count, edge2Sum);
                }
            }).join(
                tg.getEdges()
                    .join(tg.getVertices()).where(new TargetId<>()).equalTo(new Id<>())
                    .map(
                        new MapFunction<Tuple2<TemporalEdge, TemporalVertex>, Tuple3<Long, Integer, Double>>() {
                            @Override
                            public Tuple3<Long, Integer, Double> map(Tuple2<TemporalEdge, TemporalVertex> e)
                                throws Exception {
                                TemporalEdge edge1 = e.f0;
                                TemporalVertex src = e.f1;

                                long srcId = src.getPropertyValue("id").getLong();
                                int edge2Count = (int) edge1.getPropertyValue("count").getLong();
                                double edge2Sum = edge1.getPropertyValue("sum_amount").getDouble();

                                return new Tuple3<>(srcId, edge2Count, edge2Sum);
                            }
                        })
            ).where(0)
            .equalTo(0)
            .map(
                new MapFunction<Tuple2<Tuple3<Long, Integer, Double>, Tuple3<Long, Integer, Double>>, Tuple3<Integer, Integer, Double>>() {
                    @Override
                    public Tuple3<Integer, Integer, Double> map(
                        Tuple2<Tuple3<Long, Integer, Double>, Tuple3<Long, Integer, Double>> e)
                        throws Exception {
                        Tuple3<Long, Integer, Double> edge2 = e.f0;
                        Tuple3<Long, Integer, Double> edge1 = e.f1;

                        return new Tuple3<>(edge1.f1, edge2.f1, roundToDecimalPlaces(edge1.f2 / edge2.f2, 3));
                    }
                });

        List<Tuple3<Integer, Integer, Double>> edgeValuesList = null;
        try {
            edgeValuesList = edgeValues
                .collect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<ComplexRead7Result> complexRead7Results = new ArrayList<>();
        for (Tuple3<Integer, Integer, Double> edge : edgeValuesList) {
            complexRead7Results.add(new ComplexRead7Result(edge.f0, edge.f1, edge.f2.floatValue()));
        }

        return complexRead7Results;
    }
}
