package org.ldbcouncil.finbench.impls.gradoop.queries;

import java.util.HashMap;
import java.util.Map;

public class CmdArgExecutorRegistry {

    private final Map<String, CmdArgExecutor<?>> executorMap = new HashMap<>();

    /**
     * Registers a CmdArgExecutor with a query key.
     *
     * @param executor the executor to register
     */
    public void registerCmdArgExecutor(CmdArgExecutor<?> executor) {
        String key = executor.getQueryKey();
        if (executorMap.containsKey(key)) {
            throw new IllegalArgumentException("Executor with title " + key + " is already registered.");
        }
        executorMap.put(key, executor);
    }

    /**
     * Retrieves a CmdArgExecutor by its query key.
     *
     * @param queryKey the title of the query
     * @return the registered CmdArgExecutor
     * @throws IllegalArgumentException if no executor is found for the given title
     */
    public CmdArgExecutor<?> getCmdArgExecutorByTitle(String queryKey) {
        CmdArgExecutor<?> executor = executorMap.get(queryKey);
        if (executor == null) {
            throw new IllegalArgumentException("No executor found for query title: " + queryKey);
        }
        return executor;
    }

    /**
     * Retrieves all registered executors.
     *
     * @return a map of query titles to their executors
     */
    public Map<String, CmdArgExecutor<?>> getAllExecutors() {
        return new HashMap<>(executorMap);
    }
}
