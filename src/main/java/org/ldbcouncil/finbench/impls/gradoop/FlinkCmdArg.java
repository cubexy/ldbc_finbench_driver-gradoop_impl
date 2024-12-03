package org.ldbcouncil.finbench.impls.gradoop;

import java.util.Date;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;

public class FlinkCmdArg {

    private String mode;
    private String queryName;
    private String dataPath;
    private long id;
    private long id2;
    private long personId;
    private long personId2;
    private Date startTime;
    private Date endTime;
    private int threshold;
    private int threshold2;
    private int truncationLimit;
    private TruncationOrder truncationOrder;

    /**
     * Initializes the input arguments from the command line arguments.
     *
     * @param cmd command line arguments
     */
    public FlinkCmdArg(CommandLine cmd, Set<String> availableQueryNames) {
        try {
            initializeFromArgs(cmd, availableQueryNames);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Initializes the input arguments from the command line arguments.
     *
     * @param cmd command line arguments
     */
    private void initializeFromArgs(CommandLine cmd, Set<String> availableQueryNames) {
        if (!cmd.hasOption("d") || !cmd.hasOption("q") || !cmd.hasOption("m")) {
            throw new RuntimeException("Missing required arguments");
        }
        if (cmd.hasOption("q_id")) {
            setId(cmd.getOptionValue("q_id"));
        }
        if (cmd.hasOption("q_id2")) {
            setId2(cmd.getOptionValue("q_id2"));
        }
        if (cmd.hasOption("d")) {
            setDataPath(cmd.getOptionValue("d"));
        }
        if (cmd.hasOption("q")) {
            setQueryName(cmd.getOptionValue("q"), availableQueryNames);
        }
        if (cmd.hasOption("m")) {
            setMode(cmd.getOptionValue("m"));
        }
        if (cmd.hasOption("q_pid1")) {
            setPersonId(cmd.getOptionValue("q_pid1"));
        }
        if (cmd.hasOption("q_pid2")) {
            setPersonId2(cmd.getOptionValue("q_pid2"));
        }
        if (cmd.hasOption("q_st")) {
            setStartTime(cmd.getOptionValue("q_st"));
        }
        if (cmd.hasOption("q_et")) {
            setEndTime(cmd.getOptionValue("q_et"));
        }
        if (cmd.hasOption("q_ts")) {
            setThreshold(cmd.getOptionValue("q_st"));
        }
        if (cmd.hasOption("q_ts2")) {
            setThreshold2(cmd.getOptionValue("q_ts2"));
        }
        if (cmd.hasOption("q_tl")) {
            setTruncationLimit(cmd.getOptionValue("q_tl"));
        }
        if (cmd.hasOption("q_to")) {
            setTruncationOrder(cmd.getOptionValue("q_to"));
        }
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        if (!mode.equals("csv") && !mode.equals("indexed-csv") && !mode.equals("parquet") &&
            !mode.equals("parquet-protobuf")) {
            throw new RuntimeException("Invalid import mode: " + mode);
        }
        this.mode = mode;
    }

    public String getQueryName() {
        return queryName;
    }

    public void setQueryName(String queryName, Set<String> availableQueryNames) {
        if (!availableQueryNames.contains(queryName)) {
            throw new RuntimeException("Invalid query name: " + queryName);
        }

        this.queryName = queryName;
    }

    public long getId() {
        return id;
    }

    public void setId(String id) {
        this.id = Long.parseLong(id);
    }

    public long getId2() {
        return id2;
    }

    public void setId2(String id2) {
        this.id2 = Long.parseLong(id2);
    }

    public long getPersonId() {
        return personId;
    }

    public void setPersonId(String personId) {
        this.personId = Long.parseLong(personId);
    }

    public long getPersonId2() {
        return personId2;
    }

    public void setPersonId2(String personId2) {
        this.personId2 = Long.parseLong(personId2);
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = new Date(Long.parseLong(startTime));
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = new Date(Long.parseLong(endTime));
    }

    public int getThreshold() {
        return threshold;
    }

    public void setThreshold(String threshold) {
        this.threshold = Integer.parseInt(threshold);
    }

    public int getThreshold2() {
        return threshold2;
    }

    public void setThreshold2(String threshold2) {
        this.threshold2 = Integer.parseInt(threshold2);
    }

    public int getTruncationLimit() {
        return truncationLimit;
    }

    public void setTruncationLimit(String truncationLimit) {
        this.truncationLimit = Integer.parseInt(truncationLimit);
    }

    public TruncationOrder getTruncationOrder() {
        return truncationOrder;
    }

    public void setTruncationOrder(String truncationOrder) {
        this.truncationOrder = TruncationOrder.valueOf(
            truncationOrder); // truncation order can be TIMESTAMP_DESCENDING or TIMESTAMP_ASCENDING
    }

    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }
}
