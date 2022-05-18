import java.util.List;

public interface AsynchronousHeaderFileWriter extends Runnable{

    void setColumnsInfo(List<ColumnInfo> columnsInfo);

}
