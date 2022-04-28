import java.io.*;

public class InfoLoggerOnTxtFile implements InfoLogger {

    private Writer bufferedWriter;
    private boolean fileIsOpen = true;


    //TODO: ci sono problemi con campi static usando multithreading?
    private final static String UNABLE_TO_OPEN_FILE_MSG = "At %s tried to open txt file for log but " +
            "exception happened. Exception msg: %s%n";
    private final static String UNABLE_TO_WRITE_ON_FILE_MSG = "At %s tried to write on txt file for log but " +
            "exception happened. Exception msg: %s%n";
    private final static String UNABLE_TO_CLOSE_FILE_MSG = "At %s tried to write on txt file for log but " +
            "exception happened. Exception msg: %s%n";

    private final static String STANDARD_LOG_FORMAT = "At %s %s: %s%n";
    private final static String STARTED_LOG_MSG = "-----------%nAt %s started log%n-----------";


    public InfoLoggerOnTxtFile(String fileNameWithExtension, String path) {
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(path +
                    File.separator + fileNameWithExtension, true));
        } catch (IOException e) {
            System.err.printf(UNABLE_TO_OPEN_FILE_MSG, java.time.LocalDate.now(), e.toString());
            fileIsOpen = false;
        }

        if (fileIsOpen)
            this.log(STARTED_LOG_MSG, LogLevel.NORMAL);

        return;
    }

    public void log(String msg, LogLevel level) {

        if (!fileIsOpen)
            return;


        try {
            bufferedWriter.write(String.format(STANDARD_LOG_FORMAT, java.time.LocalDate.now(), level.toString(), msg));
        } catch (IOException e) {
            System.err.printf(UNABLE_TO_WRITE_ON_FILE_MSG, java.time.LocalDate.now(), e.toString());
        }


        return;
    }

    public void close() {
        if (!fileIsOpen)
            return;

        try {
            bufferedWriter.close();
        } catch (IOException e) {
            System.err.printf(UNABLE_TO_CLOSE_FILE_MSG, java.time.LocalDate.now(), e.toString());
        }

        return;
    }

}
