package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read4;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.MaxProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumProperty;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4Result;

class ComplexRead4GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead4Result>> {

    private final long startTime;
    private final long endTime;
    private final long id1;
    private final long id2;

    public ComplexRead4GradoopOperator(ComplexRead4 cr4) {
        this.startTime = cr4.getStartTime().getTime();
        this.endTime = cr4.getEndTime().getTime();
        this.id1 = cr4.getId1();
        this.id2 = cr4.getId2();
    }

    @Override
    public List<ComplexRead4Result> execute(TemporalGraph temporalGraph) {
        final long id_serializable = this.id1;
        final long id2_serializable = this.id2;

        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer"))
            .fromTo(this.startTime, this.endTime);

        TemporalGraph otherAccounts = windowedGraph.query(
                "MATCH (src:Account)-[edge1:transfer]->(dst:Account), (src)<-[edge2:transfer]-(other:Account)<-[edge3:transfer]-(dst) WHERE src.id = " +
                    this.id1 + "L AND dst.id = " + this.id2 + "L")
            .reduce(new ReduceCombination<>())
            .transformVertices((currentVertex, transformedVertex) -> {
                if (currentVertex.hasProperty("id") && (
                    Objects.equals(currentVertex.getPropertyValue("id").getLong(), id_serializable)
                        || Objects.equals(currentVertex.getPropertyValue("id").getLong(), id2_serializable)
                    )) {
                    currentVertex.removeProperty("id");
                }
                return currentVertex;
            })
            .callForGraph(
                new KeyedGrouping<>(Arrays.asList(GroupingKeys.label(), GroupingKeys.property("id")),
                    null, null,
                    Arrays.asList(new Count("count"), new SumProperty("amount"), new MaxProperty("amount")))
            );

        List<Tuple7<Long, Integer, Double, Double, Integer, Double, Double>> edgeMap = null;

        try {
            MapOperator<Tuple2<Tuple4<Long, Integer, Double, Double>, Tuple4<Long, Integer, Double, Double>>, Tuple7<Long, Integer, Double, Double, Integer, Double, Double>>
                edges = otherAccounts.getEdges()
                .join(otherAccounts.getVertices()).where(new SourceId<>()).equalTo(new Id<>())
                .map(new MapFunction<Tuple2<TemporalEdge, TemporalVertex>, Tuple4<Long, Integer, Double, Double>>() {
                    @Override
                    public Tuple4<Long, Integer, Double, Double> map(Tuple2<TemporalEdge, TemporalVertex> e)
                        throws Exception {
                        TemporalEdge edge2 = e.f0;
                        TemporalVertex src = e.f1;

                        long otherID = src.getPropertyValue("id").getLong();
                        int numEdges = (int) edge2.getPropertyValue("count").getLong();
                        double sumAmount = roundToDecimalPlaces(edge2.getPropertyValue("sum_amount").getDouble(), 3);
                        double maxAmount = roundToDecimalPlaces(edge2.getPropertyValue("max_amount").getDouble(), 3);

                        return new Tuple4<>(otherID, numEdges, sumAmount, maxAmount);
                    }
                }).join(
                    otherAccounts.getEdges()
                        .join(otherAccounts.getVertices()).where(new TargetId<>()).equalTo(new Id<>())
                        .map(
                            new MapFunction<Tuple2<TemporalEdge, TemporalVertex>, Tuple4<Long, Integer, Double, Double>>() {
                                @Override
                                public Tuple4<Long, Integer, Double, Double> map(Tuple2<TemporalEdge, TemporalVertex> e)
                                    throws Exception {
                                    TemporalEdge edge3 = e.f0;
                                    TemporalVertex src = e.f1;

                                    long otherID = src.getPropertyValue("id").getLong();
                                    int numEdges = (int) edge3.getPropertyValue("count").getLong();
                                    double sumAmount =
                                        roundToDecimalPlaces(edge3.getPropertyValue("sum_amount").getDouble(), 3);
                                    double maxAmount =
                                        roundToDecimalPlaces(edge3.getPropertyValue("max_amount").getDouble(), 3);

                                    return new Tuple4<>(otherID, numEdges, sumAmount, maxAmount);
                                }
                            })
                ).where(0)
                .equalTo(0)
                .map(
                    new MapFunction<Tuple2<Tuple4<Long, Integer, Double, Double>, Tuple4<Long, Integer, Double, Double>>, Tuple7<Long, Integer, Double, Double, Integer, Double, Double>>() {
                        @Override
                        public Tuple7<Long, Integer, Double, Double, Integer, Double, Double> map(
                            Tuple2<Tuple4<Long, Integer, Double, Double>, Tuple4<Long, Integer, Double, Double>> e)
                            throws Exception {
                            Tuple4<Long, Integer, Double, Double> edge2 = e.f0;
                            Tuple4<Long, Integer, Double, Double> edge3 = e.f1;

                            return new Tuple7<>(edge2.f0, edge2.f1, edge2.f2, edge2.f3, edge3.f1, edge3.f2, edge3.f3);
                        }
                    });

            windowedGraph.getConfig().getExecutionEnvironment().setParallelism(1);

            edgeMap = edges
                .sortPartition(2, Order.DESCENDING)
                .sortPartition(5, Order.ASCENDING)
                .sortPartition(0, Order.ASCENDING)
                .collect();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // if no results are found, there are no transfers between src and dst --> return empty list
        if (edgeMap.isEmpty()) { return Collections.emptyList(); }

        List<ComplexRead4Result> complexRead4Results = new ArrayList<>();
        for (Tuple7<Long, Integer, Double, Double, Integer, Double, Double> edge : edgeMap) {
            complexRead4Results.add(new ComplexRead4Result(edge.f0, edge.f1, edge.f2, edge.f3, edge.f4, edge.f5, edge.f6));
        }

        return complexRead4Results;
    }
}
