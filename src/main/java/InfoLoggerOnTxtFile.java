import java.io.*;

public class InfoLoggerOnTxtFile implements InfoLogger {

    private Writer bufferedWriter;
    private boolean fileIsOpen = true;

    private final static String UNABLE_TO_OPEN_FILE_MSG = "At %s tried to open txt file for log but " +
            "exception happened. Exception msg: %s%n";
    private final static String UNABLE_TO_WRITE_ON_FILE_MSG = "Tried to write on txt file for log but " +
            "exception happened. Exception msg: %s%n";
    private final static String UNABLE_TO_CLOSE_FILE_MSG = "Tried to write on txt file for log but " +
            "exception happened. Exception msg: %s. For now on the log will be print on stdout%n";

    private final static String STANDARD_LOG_FORMAT = "At %s %s: %s%n";
    private final static String STARTED_LOG_MSG = "%n%n%n%n-----------%n-----------%n-----------%n" +
            "At %s started log" +
            "-----------%n-----------%n-----------%n";

    public InfoLoggerOnTxtFile(String fileFullPath) {

        fileFullPath += (fileFullPath.endsWith(".txt") ? "" : ".txt");

        try {
            bufferedWriter = new BufferedWriter(new FileWriter(fileFullPath, true));
        } catch (IOException e) {
            System.err.printf(UNABLE_TO_OPEN_FILE_MSG, java.time.LocalDate.now(), e.toString());
            fileIsOpen = false;
        }

        if (fileIsOpen)
            this.log(STARTED_LOG_MSG, LogLevel.NORMAL);

        return;
    }

    public void log(String msg, LogLevel level) {

        if (!fileIsOpen){
            System.out.printf(STANDARD_LOG_FORMAT, java.time.LocalDateTime.now(), level.toString(), msg);
            return;
        }

        try {
            bufferedWriter.write(String.format(STANDARD_LOG_FORMAT, java.time.LocalDateTime.now(), level.toString(), msg));
            bufferedWriter.flush();
        } catch (IOException e) {
            System.err.printf(UNABLE_TO_WRITE_ON_FILE_MSG, e.toString());
            this.close();
            fileIsOpen = false;
        }

        return;
    }

    public void close() {

        if (!fileIsOpen)
            return;

        try {
            bufferedWriter.close();
        } catch (IOException e) {
            System.err.printf(UNABLE_TO_CLOSE_FILE_MSG, e.toString());
        } finally {
            fileIsOpen = false;
        }

        return;
    }

}