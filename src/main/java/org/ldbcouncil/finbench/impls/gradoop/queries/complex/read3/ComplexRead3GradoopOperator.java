package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read3;

import java.util.List;
import org.gradoop.flink.algorithms.gelly.shortestpaths.SingleSourceShortestPaths;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead3Result;

class ComplexRead3GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, ComplexRead3Result> {

    private final long startTime;
    private final long endTime;
    private final long id1;
    private final long id2;

    public ComplexRead3GradoopOperator(ComplexRead3 cr3) {
        this.startTime = cr3.getStartTime().getTime();
        this.endTime = cr3.getEndTime().getTime();
        this.id1 = cr3.getId1();
        this.id2 = cr3.getId2();
    }

    @Override
    public ComplexRead3Result execute(TemporalGraph temporalGraph) {
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer"))
            .fromTo(this.startTime, this.endTime);

        TemporalGraph weightedGraph = windowedGraph.transformEdges((e1, e2) -> {
            e1.setProperty("weight", 1);
            return e1;
        });

        TemporalVertex src = null;

        try {
            src =
                weightedGraph.getVertices()
                    .filter(ver -> ver.hasProperty("id"))
                    .filter(ver -> ver.getPropertyValue("id").getLong() == this.id1).collect()
                    .get(0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        TemporalGraph SSSPGraph = weightedGraph.callForGraph(
            new SingleSourceShortestPaths<>(
                src.getId(),
                "weight",
                100,
                "distance"
            )
        );

        List<TemporalVertex> dst = null;

        try {
            dst = SSSPGraph.getVertices()
                    .filter( ver -> ver.hasProperty("id"))
                    .filter(ver -> ver.getPropertyValue("id").getLong() == this.id2).collect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (dst.isEmpty()) { return new ComplexRead3Result(-1); }

        long distance = dst.get(0).getPropertyValue("distance").getLong();

        return new ComplexRead3Result(distance);
    }
}
