import java.io.*;
import java.util.List;
import java.util.Objects;

public class FailedFilesPrinter implements AsynchronousFailedFilesManager {

    private List<String> filesNames;
    private InfoLogger infoLogger;

    private final String FAILED_FILES_LIST_MSG = "The netcdf which failed preprocessing are: %s";
    private final LogLevel FAILED_FILES_LIST_LOG_LVL = LogLevel.WARNING;

    private final String NO_FILE_NAME_SET_MSG = "The program didn't set the failed files names" +
            "before running the thread";
    private final LogLevel NO_FILE_NAME_SET_LOG_LVL = LogLevel.WARNING;

    private final String EMPTY_FAILED_LIST_MSG = "There are no files which failed preprocessing";
    private final LogLevel EMPTY_FAILED_LIST_LOG_LVL = LogLevel.NORMAL;

    public FailedFilesPrinter(InfoLogger infoLogger) {
        this.infoLogger = infoLogger;
    }

    public void run() {

        if (Objects.isNull(filesNames)) {
            infoLogger.log(NO_FILE_NAME_SET_MSG, NO_FILE_NAME_SET_LOG_LVL);
            return;
        }

        if (filesNames.size() > 0)
            infoLogger.log(String.format(FAILED_FILES_LIST_MSG, String.join(", ", filesNames)),
                    FAILED_FILES_LIST_LOG_LVL);
        else
            infoLogger.log(EMPTY_FAILED_LIST_MSG, EMPTY_FAILED_LIST_LOG_LVL);

    }

    @Override
    public void setFailedFilesNames(List<String> filesNames) {
        this.filesNames = filesNames;
    }

}
