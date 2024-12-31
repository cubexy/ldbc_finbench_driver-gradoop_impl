package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read11;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;

class ComplexRead11GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead11Result>> {

    private final long id;
    private final long startTime;
    private final long endTime;
    private final int truncationLimit;
    private final boolean isTruncationOrderAscending;

    public ComplexRead11GradoopOperator(ComplexRead11 cr11) {
        this.id = cr11.getId();
        this.startTime = cr11.getStartTime().getTime();
        this.endTime = cr11.getEndTime().getTime();
        this.truncationLimit = cr11.getTruncationLimit();
        final TruncationOrder truncationOrder = cr11.getTruncationOrder();
        this.isTruncationOrderAscending = truncationOrder == TruncationOrder.TIMESTAMP_ASCENDING;
    }

    @Override
    public List<ComplexRead11Result> execute(TemporalGraph temporalGraph) {
        // TODO: implement truncation strategy
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Person", "Loan"), new LabelIsIn<>("guarantee", "apply"))
            .fromTo(this.startTime, this.endTime);

        DataSet<GraphTransaction> gtxLength3 = windowedGraph
            .temporalQuery(
                "MATCH (p1:Person)-[:guarantee]->(p2:Person)->[:guarantee]->(p3:Person)->[:guarantee]->(p4:Person), (p2)->[a:apply]->(l:Loan), (p3)->[a]->(l), (p4)->[a]->(l) " +
                    " WHERE p1.id = " + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> gtxLength2 = windowedGraph
            .temporalQuery(
                "MATCH (p1:Person)-[:guarantee]->(p2:Person)->[:guarantee]->(p3:Person), (p2)->[a:apply]->(l:Loan), (p3)->[a]->(l) " +
                    " WHERE p1.id = " + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> gtxLength1 = windowedGraph
            .temporalQuery("MATCH (p1:Person)-[:guarantee]->(p2:Person)-[a:apply]->(l:Loan)" +
                " WHERE p1.id = " + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<Tuple4<Long, Integer, Long, String>> result =
            gtxLength1.union(gtxLength2).union(gtxLength3)
                .map(new MapFunction<GraphTransaction, Tuple4<Long, Integer, Long, String>>() {
                    @Override
                    public Tuple4<Long, Integer, Long, String> map(GraphTransaction graphTransaction) throws Exception {
                        Map<String, GradoopId> m = CommonUtils.getVariableMapping(graphTransaction);

                        GradoopId otherGradoopId = m.get("other");
                        GradoopId mediumGradoopId = m.get("m");

                        Long otherId =
                            graphTransaction.getVertexById(otherGradoopId).getPropertyValue("id").getLong();
                        int accountDistance = graphTransaction.getEdges().size() - 1;
                        Long mediumId =
                            graphTransaction.getVertexById(mediumGradoopId).getPropertyValue("id").getLong();
                        String mediumType =
                            graphTransaction.getVertexById(mediumGradoopId).getPropertyValue("type").getString();
                        return new Tuple4<>(otherId, accountDistance, mediumId,
                            mediumType);
                    }
                }).distinct(0, 1, 2, 3);

        windowedGraph.getConfig().getExecutionEnvironment().setParallelism(1);

        result = result
            .sortPartition(1, Order.ASCENDING)
            .sortPartition(0, Order.ASCENDING)
            .sortPartition(3, Order.ASCENDING);

        List<ComplexRead1Result> complexRead1Results = new ArrayList<>();

        try {
            result.collect().forEach(
                tuple -> complexRead1Results.add(new ComplexRead1Result(tuple.f0, tuple.f1, tuple.f2, tuple.f3)));
        } catch (Exception e) {
            throw new RuntimeException("Error while collecting results for complex read 1: " + e);
        }

        return complexRead1Results;
    }
}
