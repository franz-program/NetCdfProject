import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class CsvWriterFromMultipleFiles extends Thread implements AsynchronousTableWriter {

    private final Queue<Object[]> rowsQueue = new LinkedList<>();
    private final List<String> activeFiles = new ArrayList<>();
    private final InfoLogger infoLogger;

    private final String prefixCsvFilesName;
    private final String csvFilePath;
    private CSVPrinter printer;
    private String[] columnNames;

    private int maximumNumberOfRowsPerFile = 50000;
    private int currentNOfGeneratedFiles = 0;
    private int nOfRowsCurrentOutputFile = 0;




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

    private final String NO_COLUMNS_NAMES_SET_MSG = "The program didn't set any column name"
            + PROCESS_WILL_CONTINUE_MSG;
    private final LogLevel NO_COLUMNS_NAMES_SET_LOG_LVL = LogLevel.WARNING;


    public CsvWriterFromMultipleFiles(List<String> activeFiles, InfoLogger infoLogger,
                                      String prefixCsvFilesName, String csvFilePath) {
        //TODO: devo copiarlo o posso lasciare lo stesso anche se questa classe cancella gli elementi?
        Collections.copy(this.activeFiles, activeFiles);
        this.infoLogger = infoLogger;

        if (prefixCsvFilesName.endsWith(".csv"))
            this.prefixCsvFilesName = prefixCsvFilesName;
        else
            this.prefixCsvFilesName = prefixCsvFilesName + ".csv";

        this.csvFilePath = csvFilePath;

    }

    @Override
    public void startTask() {
        this.start();
    }

    public void run() {

        infoLogger.log(STARTED_WRITING_MSG, STARTED_WRITING_LOG_LVL);

        if(Objects.isNull(columnNames)){
            columnNames = new String[]{"no name was set for columns"};
            infoLogger.log(NO_COLUMNS_NAMES_SET_MSG, NO_COLUMNS_NAMES_SET_LOG_LVL);
        }

        openNewCsvFile();

        while (activeFiles.size() > 0) {
            if (rowsQueue.size() > 0) {
                writeRows();
            }
            //TODO: fare uno sleep?
        }
        flushAndCloseCsvFile();
    }

    private void writeRows() {

        while (true) {
            try {
                printer.printRecord(rowsQueue.remove());
                nOfRowsCurrentOutputFile++;

                if (nOfRowsCurrentOutputFile >= maximumNumberOfRowsPerFile)
                    openNewCsvFile();

            } catch (IOException e) {
                //TODO: chiudo file e chiudo tutti i thread?
                infoLogger.log(String.format(COULDNT_WRITE_ON_CSV_MSG, getCurrentCsvFileFullName()),
                        COULDNT_WRITE_ON_CSV_LOG_LVL);
            } catch (NoSuchElementException e) {
                break;
            }
        }
        return;
    }

    public void writeRow(Object[] row) {

        //TODO: serve synchronized?
        synchronized (this) {
            //TODO: da vedere il discorso coda piena
            rowsQueue.add(row);
        }

        return;
    }

    public void sourceHasFinished(String fileName) {

        activeFiles.remove(fileName);
        //TODO: chiudo file?

        return;
    }

    private void flushAndCloseCsvFile() {

        try {
            printer.flush();
        } catch (IOException e) {
            //TODO: chiudo file e i thread?
            infoLogger.log(String.format(COULDNT_WRITE_ON_CSV_MSG, getCurrentCsvFileFullName()),
                    COULDNT_WRITE_ON_CSV_LOG_LVL);
        }

        try {
            printer.close();
        } catch (IOException e) {
            //TODO: chiudo file e i thread?
            infoLogger.log(String.format(COULDNT_CLOSE_CSV_MSG, getCurrentCsvFileFullName()),
                    COULDNT_CLOSE_CSV_LOG_LVL);
        }

    }

    private void openNewCsvFile() {

        if (!Objects.isNull(printer)) {
            flushAndCloseCsvFile();
            nOfRowsCurrentOutputFile = 0;
            currentNOfGeneratedFiles++;
        }

        //TODO: log

        try {
            printer = CSVFormat.DEFAULT.withHeader(columnNames).print(Files.newBufferedWriter(
                    Paths.get(getCurrentCsvFileFullPath())
            ));
        } catch (IOException e) {
            //TODO: chiudo file e i thread?
            infoLogger.log(String.format(COULDNT_OPEN_CSV_MSG, getCurrentCsvFileFullName()),
                    COULDNT_OPEN_CSV_LOG_LVL);
        }
        return;
    }

    public void setMaximumNumberOfRowsPerFile(int max) {
        if (max >= 1)
            nOfRowsCurrentOutputFile = max;
        return;
    }

    @Override
    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    private String getCurrentCsvFileFullName() {
        return prefixCsvFilesName.substring(0, prefixCsvFilesName.length() - 4) + currentNOfGeneratedFiles + ".csv";
    }

    private String getCurrentCsvFileFullPath() {
        return csvFilePath + File.separator + getCurrentCsvFileFullName();
    }
    

}
