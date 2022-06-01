public interface DataGetter {

    int getNOfRows();

    float[] getNextRow();

    void readFromBeginning();

    String getSourceName();

}
