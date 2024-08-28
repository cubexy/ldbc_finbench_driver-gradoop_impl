package org.ldbcouncil.finbench.impls.gradoop.queries.read.simple;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.getDataSetMax;
import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.getDataSetSum;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class SimpleRead2Handler implements OperationHandler<SimpleRead2, GradoopFinbenchBaseGraphState> {
    @Override
    public void executeOperation(SimpleRead2 sr2, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(sr2.toString());
        try {
            Tuple6<Float, Float, Long, Float, Float, Long> sr2Result =
                new SimpleRead2GradoopOperator(sr2).execute(connectionState.getGraph());
            List<SimpleRead2Result> simpleRead2Results = new ArrayList<>();
            simpleRead2Results.add(new SimpleRead2Result(sr2Result.f0, sr2Result.f1, sr2Result.f2, sr2Result.f3, sr2Result.f4, sr2Result.f5));
            resultReporter.report(simpleRead2Results.size(), simpleRead2Results, sr2);
        } catch (Exception e) {
            throw new DbException("Error while collecting results for simple read 2: " + e);
        }
    }
}

class SimpleRead2GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, Tuple6<Float, Float, Long, Float, Float, Long>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;

    public SimpleRead2GradoopOperator(SimpleRead2 sr2) {
        this.id = sr2.getId();
        this.startTime = sr2.getStartTime();
        this.endTime = sr2.getEndTime();
    }

    @Override
    public Tuple6<Float, Float, Long, Float, Float, Long> execute(TemporalGraph temporalGraph) {
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer"))
            .fromTo(this.startTime.getTime(), this.endTime.getTime()); // Get all transfers between start and end time

        DataSet<GraphTransaction> transferIns = windowedGraph
            .query(
                "MATCH (src:Account)-[edge1:transfer]->(dst1:Account) WHERE src <> dst1 AND src.id =" + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> transferOuts = windowedGraph
            .query(
                "MATCH (dst2:Account)-[edge2:transfer]->(src:Account) WHERE src <> dst2 AND src.id =" + this.id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<Tuple1<Double>> transferInsMap = transferIns
            .map(new MapFunction<GraphTransaction, Tuple1<Double>>() {
                @Override
                public Tuple1<Double> map(GraphTransaction graphTransaction) throws Exception {
                    Map<String, GradoopId> m = CommonUtils.getVariableMapping(graphTransaction);
                    GradoopId edgeId = m.get("edge1");

                    EPGMEdge edge = graphTransaction.getEdges().iterator().next(); //there always is only one edge

                    return new Tuple1<>(edge.getPropertyValue("amount").getDouble());
                }
            });

        DataSet<Tuple1<Double>> transferOutsMap = transferOuts
            .map(new MapFunction<GraphTransaction, Tuple1<Double>>() {
                @Override
                public Tuple1<Double> map(GraphTransaction graphTransaction) throws Exception {
                    Map<String, GradoopId> m = CommonUtils.getVariableMapping(graphTransaction);
                    GradoopId edgeId = m.get("edge2");

                    EPGMEdge edge = graphTransaction.getEdges().iterator().next();

                    return new Tuple1<>(edge.getPropertyValue("amount").getDouble());
                }
            });

        try {
            float transferInsSum = getDataSetSum(transferInsMap, 3);
            float transferInsMax = getDataSetMax(transferInsMap, 3);
            long transferInsCount = transferInsMap.count();
            if (transferInsCount == 0L) {
                transferInsMax = -1.0f;
            }
            float transferOutsSum = getDataSetSum(transferOutsMap, 3);
            float transferOutsMax = getDataSetMax(transferOutsMap, 3);
            long transferOutsCount = transferOutsMap.count();
            if (transferOutsCount == 0L) {
                transferOutsMax = -1.0f;
            }

            return new Tuple6<>(transferInsSum, transferInsMax, transferInsCount, transferOutsSum, transferOutsMax, transferOutsCount);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}