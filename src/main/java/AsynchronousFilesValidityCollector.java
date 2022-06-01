public interface AsynchronousFilesValidityCollector extends Runnable{

    void setFileValidity(String filename, boolean isValid);

}
