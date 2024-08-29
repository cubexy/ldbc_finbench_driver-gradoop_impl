package org.ldbcouncil.finbench.impls.gradoop.queries.read.simple;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.MaxProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumProperty;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2Result;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class SimpleRead2Handler implements OperationHandler<SimpleRead2, GradoopFinbenchBaseGraphState> {
    @Override
    public void executeOperation(SimpleRead2 sr2, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(sr2.toString());
        try {
            Tuple6<Double, Double, Long, Double, Double, Long> sr2Result =
                new SimpleRead2GradoopOperator(sr2).execute(connectionState.getGraph());
            List<SimpleRead2Result> simpleRead2Results = new ArrayList<>();
            simpleRead2Results.add(
                new SimpleRead2Result(sr2Result.f0, sr2Result.f1, sr2Result.f2, sr2Result.f3, sr2Result.f4,
                    sr2Result.f5));
            resultReporter.report(simpleRead2Results.size(), simpleRead2Results, sr2);
        } catch (Exception e) {
            throw new DbException("Error while collecting results for simple read 2: " + e);
        }
    }
}

class SimpleRead2GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, Tuple6<Double, Double, Long, Double, Double, Long>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;

    public SimpleRead2GradoopOperator(SimpleRead2 sr2) {
        this.id = sr2.getId();
        this.startTime = sr2.getStartTime();
        this.endTime = sr2.getEndTime();
    }

    @Override
    public Tuple6<Double, Double, Long, Double, Double, Long> execute(TemporalGraph temporalGraph) {
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer"))
            .fromTo(this.startTime.getTime(), this.endTime.getTime()); // Get all transfers between start and end time

        try {
            final Long id = this.id;
            List<TemporalEdge> edges = windowedGraph.query(
                    "MATCH (src:Account)-[edge1:transfer]->(dst1:Account) WHERE src <> dst1 AND src.id =" + this.id + "L")
                .union(windowedGraph.query(
                    "MATCH (dst2:Account)-[edge2:transfer]->(src:Account) WHERE src <> dst2 AND src.id =" + this.id +
                        "L"))
                .reduce(new ReduceCombination<>())
                .transformVertices((currentVertex, transformedVertex) -> {
                    if (currentVertex.hasProperty("id") &&
                        !Objects.equals(currentVertex.getPropertyValue("id").getLong(), id)) {
                        currentVertex.removeProperty("id");
                    }
                    return currentVertex;
                }).callForGraph(
                    new KeyedGrouping<>(Arrays.asList(GroupingKeys.label(), GroupingKeys.property("id")), null, null,
                        Arrays.asList(new Count("count"), new SumProperty("amount"), new MaxProperty("amount")))
                )
                .getEdges()
                .collect();
            
            final TemporalEdge transferIns = edges.get(0);
            final TemporalEdge transferOuts = edges.get(1);

            final double transferOutSum = roundToDecimalPlaces(transferOuts.getPropertyValue("sum_amount").getDouble(), 3);
            final double transferInSum = roundToDecimalPlaces(transferIns.getPropertyValue("sum_amount").getDouble(), 3);
            double transferInMax = roundToDecimalPlaces(transferIns.getPropertyValue("max_amount").getDouble(), 3);
            double transferOutMax = roundToDecimalPlaces(transferOuts.getPropertyValue("max_amount").getDouble(), 3);
            final long transferInCount = transferIns.getPropertyValue("count").getLong();
            final long transferOutCount = transferOuts.getPropertyValue("count").getLong();

            if (transferInMax == 0.0f) {
                transferInMax = -1.0f;
            }

            if (transferOutMax == 0.0f) {
                transferOutMax = -1.0f;
            }
            
            return new Tuple6<>(
                transferOutSum,
                transferOutMax,
                transferOutCount,
                transferInSum,
                transferInMax,
                transferInCount);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}