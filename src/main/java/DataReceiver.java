public interface DataReceiver {

    void sendRow(float[] row);

    DataGetter getDataGetter();

    void setSourceName(String sourceName);

}
