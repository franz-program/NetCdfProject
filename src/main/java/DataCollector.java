import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class DataCollector implements DataReceiver, DataGetter {

    private float[][] rows;
    private int nOfRows;
    private int currentWritingIndex = 0;
    private int currentReadingIndex = 0;
    private String sourceName;

    public DataCollector(int nOfRows){
        if(nOfRows < 0)
            throw new IllegalArgumentException("The number of rows can't be negative");

        rows = new float[nOfRows][];
        this.nOfRows = nOfRows;
    }

    @Override
    public void readFromBeginning() {
        currentReadingIndex = 0;
    }

    @Override
    public float[] getNextRow() {
        if (currentReadingIndex >= nOfRows)
            throw new NoSuchElementException("There aren't anymore rows to get");

        currentReadingIndex++;
        return rows[currentReadingIndex - 1];
    }

    @Override
    public void sendRow(float[] row) {

        if (currentWritingIndex >= nOfRows)
            throw new IndexOutOfBoundsException("The collector is full, no more rows are accepted");

        rows[currentWritingIndex] = row;
        currentWritingIndex++;
    }

    @Override
    public DataGetter getDataGetter() {
        return (DataGetter) this;
    }

    @Override
    public int getNOfRows() {
        return nOfRows;
    }

    @Override
    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public String getSourceName(){ return sourceName; }

}
