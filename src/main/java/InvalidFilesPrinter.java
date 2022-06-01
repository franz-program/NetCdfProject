public class InvalidFilesPrinter implements AsynchronousFilesValidityCollector {

    private InfoLogger infoLogger;
    private AsynchronousOutputFileWriter outputFileWriter;

    private final String INCORRECT_FILE_STRUCTURE_MSG = "The file %s seems not to follow the correct internal" +
            " structure";
    private final LogLevel INCORRECT_FILE_STRUCTURE_LOG_LVL = LogLevel.WARNING;

    private final static String TEST_PASSED_MSG = "The file %s has passed preprocessing";
    private final static LogLevel TEST_PASSED_LOG_LVL = LogLevel.NORMAL;

    public InvalidFilesPrinter(InfoLogger infoLogger, AsynchronousOutputFileWriter outputFileWriter) {
        this.infoLogger = infoLogger;
        this.outputFileWriter = outputFileWriter;
    }

    public void run() {
        //TODO: se vuoi fai un file txt che scriva quali file hanno/non hanno passato il test
        return;
    }


    @Override
    public void setFileValidity(String filename, boolean isValid) {

        if (isValid)
            infoLogger.log(String.format(TEST_PASSED_MSG, filename), TEST_PASSED_LOG_LVL);
        else
            infoLogger.log(String.format(INCORRECT_FILE_STRUCTURE_MSG, filename), INCORRECT_FILE_STRUCTURE_LOG_LVL);

        if (!isValid)
            outputFileWriter.sourceHasFinished(filename);
    }


}
