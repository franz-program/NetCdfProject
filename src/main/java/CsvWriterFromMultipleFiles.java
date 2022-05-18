import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CsvWriterFromMultipleFiles extends Thread implements NetcdfRowsManager {

    private final BlockingQueue<Object[]> rowsQueue = new LinkedBlockingQueue<>();
    private List<String> activeFiles;
    private final List<String> filesWhichFailed = new ArrayList<>();
    private final InfoLogger infoLogger;
    private final PostWritingPhaseManager postWritingPhaseManager;

    private final String csvFileFullPath;
    private CSVPrinter printer;
    private String[] columnNames;

    //TODO: aggiorna i messaggi d'errore per dire se uccidi tutto il processo oppure continui

    private final String PROCESS_WILL_CONTINUE_MSG = ", processing will still continue";
    private final String PROCESS_WILL_BE_INTERRUPTED_MSG = ", process will be killed";

    private final String COULDNT_WRITE_ON_CSV_MSG = "The program tried to write in %s but failed";
    private final LogLevel COULDNT_WRITE_ON_CSV_LOG_LVL = LogLevel.ERROR;

    private final String COULDNT_CLOSE_CSV_MSG = "The program tried to close %s but failed";
    private final LogLevel COULDNT_CLOSE_CSV_LOG_LVL = LogLevel.ERROR;

    private final String COULDNT_OPEN_CSV_MSG = "The program tried to open %s but failed";
    private final LogLevel COULDNT_OPEN_CSV_LOG_LVL = LogLevel.ERROR;

    private final String STARTED_WRITING_MSG = "The program started writing into csv(s)";
    private final LogLevel STARTED_WRITING_LOG_LVL = LogLevel.NORMAL;

    private final String NEW_FILE_CREATED_MSG = "A new csv file (%s) has been created";
    private final LogLevel NEW_FILE_CREATED_LOG_LVL = LogLevel.NORMAL;

    private final String NO_COLUMNS_NAMES_SET_MSG = "The program didn't set any column name"
            + PROCESS_WILL_CONTINUE_MSG;
    private final LogLevel NO_COLUMNS_NAMES_SET_LOG_LVL = LogLevel.WARNING;

    private final String NO_FILE_NAME_WAS_SET_MSG = "The writer was started but no file name was set, procedure will abort";
    private final LogLevel NO_FILE_NAME_WAS_SET_LOG_LVL = LogLevel.ERROR;


    public CsvWriterFromMultipleFiles(InfoLogger infoLogger,
                                      String csvFileFullPath,
                                      PostWritingPhaseManager postWritingPhaseManager) {

        this.infoLogger = infoLogger;
        this.postWritingPhaseManager = postWritingPhaseManager;
        this.csvFileFullPath = csvFileFullPath.endsWith(".csv") ?
                csvFileFullPath : csvFileFullPath + ".csv";

    }

    @Override
    public void run() {

        if (activeFiles == null) {
            infoLogger.log(NO_FILE_NAME_WAS_SET_MSG, NO_FILE_NAME_WAS_SET_LOG_LVL);
            postWritingPhaseManager.notifyWritingHasFinished();
            return;
        }

        infoLogger.log(STARTED_WRITING_MSG, STARTED_WRITING_LOG_LVL);

        if (columnNames == null) {
            columnNames = new String[]{"no name was internally set for columns"};
            infoLogger.log(NO_COLUMNS_NAMES_SET_MSG, NO_COLUMNS_NAMES_SET_LOG_LVL);
        }

        try {
            openCsvFile();
        } catch (IOException e) {
            postWritingPhaseManager.notifyWritingHasFinished();
            return;
        }

        while (activeFiles.size() > 0 || rowsQueue.size() > 0) {
            writeRows();
        }


        flushAndCloseCsvFile();

        if (filesWhichFailed.size() > 0) {
            //TODO:
        }

        postWritingPhaseManager.notifyWritingHasFinished();
        return;
    }

    private void writeRows() {

        while (rowsQueue.size() > 0) {
            try {

                Object[] row = rowsQueue.poll(5, TimeUnit.SECONDS);
                if (row != null)
                    printer.printRecord(row);

            } catch (IOException e) {
                //TODO: chiudo file e notifico postWriterPhaseManager?
                infoLogger.log(String.format(COULDNT_WRITE_ON_CSV_MSG, csvFileFullPath),
                        COULDNT_WRITE_ON_CSV_LOG_LVL);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return;
    }

    public void addRow(Object[] row) {

        //TODO: c'è un modo più veloce della coda?

        try {
            rowsQueue.put(row);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return;
    }

    private void openCsvFile() throws IOException {

        infoLogger.log(String.format(NEW_FILE_CREATED_MSG, csvFileFullPath),
                NEW_FILE_CREATED_LOG_LVL);

        try {
            printer = CSVFormat.DEFAULT.withHeader(columnNames).print(Files.newBufferedWriter(
                    Paths.get(csvFileFullPath)
            ));
        } catch (IOException e) {
            infoLogger.log(String.format(COULDNT_OPEN_CSV_MSG, csvFileFullPath),
                    COULDNT_OPEN_CSV_LOG_LVL);
            throw e;
        }

        return;
    }

    private void flushAndCloseCsvFile() {

        try {
            printer.flush();
        } catch (IOException e) {
            //TODO: faccio notify del postWritingFileManager?
            infoLogger.log(String.format(COULDNT_WRITE_ON_CSV_MSG, csvFileFullPath),
                    COULDNT_WRITE_ON_CSV_LOG_LVL);
        }

        try {
            printer.close();
        } catch (IOException e) {
            //TODO: faccio notify del postWritingFileManager?
            infoLogger.log(String.format(COULDNT_CLOSE_CSV_MSG, csvFileFullPath),
                    COULDNT_CLOSE_CSV_LOG_LVL);
        }

    }

    public void sourceHasFinished(String fileName) {
        activeFiles.remove(fileName);
        return;
    }

    @Override
    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public void setSourceNames(List<String> fileNames) {
        activeFiles = new ArrayList<>(fileNames);
    }

    public void sourceHasFailed(String fileName) {
        filesWhichFailed.add(fileName);
        sourceHasFinished(fileName);
    }


}
