public interface AsynchronousOutputFileWriter extends Runnable{

    void addDataGetter(DataGetter dataGetter);

    void sourceHasFinished(String sourceName);

}
