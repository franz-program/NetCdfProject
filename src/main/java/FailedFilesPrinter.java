public class FailedFilesPrinter implements AsynchronousFailedFilesManager{

    private final AsynchronousOutputFileWriter outputFileWriter;
    private final InfoLogger infoLogger;

    private final static String FILE_WILL_BE_IGNORED_MSG = "The file %s will not be processed";
    private final static LogLevel FILE_WILL_BE_IGNORED_LOG_LVL = LogLevel.WARNING;

    public FailedFilesPrinter(InfoLogger infoLogger, AsynchronousOutputFileWriter outputFileWriter){
        this.infoLogger = infoLogger;
        this.outputFileWriter = outputFileWriter;
    }

    @Override
    public void setFailedFile(String filePath) {
        outputFileWriter.sourceHasFinished(filePath);
        infoLogger.log(String.format(FILE_WILL_BE_IGNORED_MSG, filePath), FILE_WILL_BE_IGNORED_LOG_LVL);
    }

    @Override
    public void run() {
        return;
    }
}
