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

    /**
     * Given two accounts and a specified time window between startTime and endTime, find the length
     * of shortest path between these two accounts by the transfer relationships. Note that all the edges
     * in the path should be in the time window and of type transfer. Return 1 if src and dst are directly
     * connected. Return -1 if there is no path found.
     *
     * @param temporalGraph input graph
     * @return 1 if src and dst are directly connected. -1 if there is no path found. otherwise, return the
     * length of shortest path between these two accounts.
     */
    @Override
    public ComplexRead3Result execute(TemporalGraph temporalGraph) {
        long serializableId1 = this.id1;
        long serializableId2 = this.id2;

        TemporalGraph weightedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer"))
            .fromTo(this.startTime, this.endTime)
            .transformEdges((e1, e2) -> {
                e1.setProperty("weight", 1); // Add weight property to edges
                return e1;
            });

        TemporalVertex src;

        try {
            src =
                weightedGraph.getVertices()
                    .filter(ver -> ver.hasProperty("id"))
                    .filter(ver -> ver.getPropertyValue("id").getLong() == serializableId1).collect()
                    .get(0); // Get account vertex (findFirst instead of filter would be more efficient)
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        TemporalGraph SSSPGraph = weightedGraph.callForGraph(
            new SingleSourceShortestPaths<>( // Apply SSSP algorithm
                src.getId(),
                "weight",
                100,
                "distance"
            )
        );

        List<TemporalVertex> dst;

        try {
            dst = SSSPGraph.getVertices()
                .filter(ver -> ver.hasProperty("id"))
                .filter(ver -> ver.getPropertyValue("id").getLong() == serializableId2).collect(); // Get vertex and distance
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (dst.isEmpty()) { // not connected
            return new ComplexRead3Result(-1);
        }

        long distance = (long) dst.get(0).getPropertyValue("distance").getDouble();

        return new ComplexRead3Result(distance);
    }
}
