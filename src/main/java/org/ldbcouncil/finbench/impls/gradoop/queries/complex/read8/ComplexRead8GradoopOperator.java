package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read8;

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
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1Result;
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

    @Override
    public List<ComplexRead8Result> execute(TemporalGraph temporalGraph) {
        // TODO: implement truncation strategy
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account", "Medium"), new LabelIsIn<>("transfer", "withdraw"))
            .fromTo(this.startTime, this.endTime);

        DataSet<GraphTransaction> gtxLength3ttt = windowedGraph
            .temporalQuery(
                "MATCH (loan:Loan)-[edge1:transfer]->(src:Account)-[edge2:transfer]->(dst:Account)-[edge3:transfer]->(dst2:Account)-[edge4:transfer]->(dst3:Account)" +
                    " WHERE loan.id = " + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> gtxLength3wtt = windowedGraph
            .temporalQuery(
                "MATCH (loan:Loan)-[edge1:transfer]->(src:Account)-[edge2:withdraw]->(dst:Account)-[edge3:transfer]->(dst2:Account)-[edge4:transfer]->(dst3:Account)" +
                    " WHERE loan.id = " + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> gtxLength3twt = windowedGraph
            .temporalQuery(
                "MATCH (loan:Loan)-[edge1:transfer]->(src:Account)-[edge2:transfer]->(dst:Account)-[edge3:withdraw]->(dst2:Account)-[edge4:transfer]->(dst3:Account)" +
                    " WHERE loan.id = " + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> gtxLength3ttw = windowedGraph
            .temporalQuery(
                "MATCH (loan:Loan)-[edge1:transfer]->(src:Account)-[edge2:transfer]->(dst:Account)-[edge3:transfer]->(dst2:Account)-[edge4:withdraw]->(dst3:Account)" +
                    " WHERE loan.id = " + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> gtxLength3wwt = windowedGraph
            .temporalQuery(
                "MATCH (loan:Loan)-[edge1:transfer]->(src:Account)-[edge2:withdraw]->(dst:Account)-[edge3:withdraw]->(dst2:Account)-[edge4:transfer]->(dst3:Account)" +
                    " WHERE loan.id = " + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> gtxLength3tww = windowedGraph
            .temporalQuery(
                "MATCH (loan:Loan)-[edge1:transfer]->(src:Account)-[edge2:transfer]->(dst:Account)-[edge3:withdraw]->(dst2:Account)-[edge4:withdraw]->(dst3:Account)" +
                    " WHERE loan.id = " + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> gtxLength3wtw = windowedGraph
            .temporalQuery(
                "MATCH (loan:Loan)-[edge1:transfer]->(src:Account)-[edge2:withdraw]->(dst:Account)-[edge3:transfer]->(dst2:Account)-[edge4:withdraw]->(dst3:Account)" +
                    " WHERE loan.id = " + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> gtxLength3www = windowedGraph
            .temporalQuery(
                "MATCH (loan:Loan)-[edge1:transfer]->(src:Account)-[edge2:withdraw]->(dst:Account)-[edge3:withdraw]->(dst2:Account)-[edge4:withdraw]->(dst3:Account)" +
                    " WHERE loan.id = " + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> gtxLength2tt = windowedGraph
            .temporalQuery(
                "MATCH (loan:Loan)-[edge1:transfer]->(src:Account)-[edge2:transfer]->(dst:Account)-[edge3:transfer]->(dst2:Account)" +
                    " WHERE loan.id = " + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> gtxLength2tw = windowedGraph
            .temporalQuery(
                "MATCH (loan:Loan)-[edge1:transfer]->(src:Account)-[edge2:transfer]->(dst:Account)-[edge3:withdraw]->(dst2:Account)" +
                    " WHERE loan.id = " + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> gtxLength2wt = windowedGraph
            .temporalQuery(
                "MATCH (loan:Loan)-[edge1:transfer]->(src:Account)-[edge2:withdraw]->(dst:Account)-[edge3:transfer]->(dst2:Account)" +
                    " WHERE loan.id = " + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> gtxLength2ww = windowedGraph
            .temporalQuery(
                "MATCH (loan:Loan)-[edge1:transfer]->(src:Account)-[edge2:withdraw]->(dst:Account)-[edge3:withdraw]->(dst2:Account)" +
                    " WHERE loan.id = " + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> gtxLength1t = windowedGraph
            .temporalQuery(
                "MATCH (loan:Loan)-[edge1:transfer]->(src:Account)-[edge2:transfer]->(dst:Account)" +
                    " WHERE loan.id = " + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> gtxLength1w = windowedGraph
            .temporalQuery(
                "MATCH (loan:Loan)-[edge1:transfer]->(src:Account)-[edge2:withdraw]->(dst:Account)" +
                    " WHERE loan.id = " + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<Tuple4<Long, Integer, Long, String>> result =
            gtxLength1t.union(gtxLength1w).union(gtxLength2tt).union(gtxLength2tw).union(gtxLength2wt).union(gtxLength2ww).union(gtxLength3ttt).union(gtxLength3ttw).union(gtxLength3twt).union(gtxLength3wtt).union(gtxLength3tww).union(gtxLength3wtw).union(gtxLength3wwt).union(gtxLength3www)
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

        return null;
    }
}
