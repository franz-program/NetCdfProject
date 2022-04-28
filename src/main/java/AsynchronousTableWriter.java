public interface AsynchronousTableWriter {

    void writeRow(Object... objects);

    void sourceHasFinished(String sourceName);

    void sourceHasFailed(String sourceName);

    //lo tengo?
    void setMaximumNumberOfRowsPerFile(int max);

    void startTask();

    void setColumnNames(String[] columnsNames);


}
