import java.util.List;

public interface NetcdfRowsManager extends Runnable{

    void addRow(Object... objects);

    void sourceHasFinished(String sourceName);

    void sourceHasFailed(String sourceName);

    void setColumnNames(String[] columnsNames);

    void setSourceNames(List<String> sourceNames);

}
