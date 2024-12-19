package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read3;

import java.util.Date;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.flink.algorithms.gelly.shortestpaths.SingleSourceShortestPaths;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead3Result;

class ComplexRead3GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead3Result>> {

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
    public List<ComplexRead3Result> execute(TemporalGraph temporalGraph) {
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer"))
            .fromTo(this.startTime, this.endTime);

        TemporalGraph weightedGraph = windowedGraph.transformEdges((e1, e2) -> {
            e1.setProperty("weight", 1);
            return e1;
        });

        TemporalVertex v = null;

        try {
            v =
                weightedGraph.getVertices()
                    .filter( ver -> ver.hasProperty("id") )
                    .filter( ver -> ver.getPropertyValue("id").getLong() == this.id1 ).collect()
                    .get(0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        TemporalGraph SSSPGraph = weightedGraph.callForGraph(
            new SingleSourceShortestPaths<>(
                // The source vertex id
                v.getId(),
                // The name of the weiht property of the edges
                "weight",
                // The number of iterations
                20,
                // The property value, where the distance is stored in the result
                "distance"));



        return null;
    }
}
