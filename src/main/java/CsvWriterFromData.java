import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CsvWriterFromData extends Thread implements AsynchronousOutputFileWriter {

    private final BlockingQueue<DataGetter> fileDataQueue = new LinkedBlockingQueue<>(1);
    private final List<String> sourceFiles;
    private final InfoLogger infoLogger;
    private final PostWritingPhaseManager postWritingPhaseManager;
    private final HeaderFileWriter headerFileWriter;

    private final String csvFileFullPath;
    private CSVPrinter printer;
    private final String[] columnNamesForData;

    private final String nameOfColumnForSourcesNames = ClassForCostants.timeVarName;
    private final boolean putFileNameAsFirstColumn;

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

    public CsvWriterFromData(InfoLogger infoLogger, String csvFileFullPath,
                             PostWritingPhaseManager postWritingPhaseManager,
                             List<String> sourceFiles, String[] columnNamesForData,
                             HeaderFileWriter headerFileWriter, boolean putFileNameAsFirstColumn) {

        this.infoLogger = infoLogger;
        this.postWritingPhaseManager = postWritingPhaseManager;
        this.csvFileFullPath = csvFileFullPath.endsWith(".csv") ?
                csvFileFullPath : csvFileFullPath + ".csv";
        this.sourceFiles = new ArrayList<>(sourceFiles);
        this.columnNamesForData = columnNamesForData;
        this.headerFileWriter = headerFileWriter;
        this.putFileNameAsFirstColumn = putFileNameAsFirstColumn;

    }

    @Override
    public void run() {

        infoLogger.log(STARTED_WRITING_MSG, STARTED_WRITING_LOG_LVL);

        try {
            openCsvFile();
        } catch (IOException e) {
            infoLogger.log(String.format(COULDNT_OPEN_CSV_MSG, csvFileFullPath), COULDNT_OPEN_CSV_LOG_LVL);
            postWritingPhaseManager.notifyWritingHasFinished();
            return;
        }

        while (sourceFiles.size() > 0 || fileDataQueue.size() > 0) {
            try {
                writeRows();
            } catch (IOException e) {
                infoLogger.log(String.format(COULDNT_WRITE_ON_CSV_MSG, csvFileFullPath),
                        COULDNT_WRITE_ON_CSV_LOG_LVL);
                flushAndCloseCsvFile();
                postWritingPhaseManager.notifyWritingHasFinished();
                return;
            }
        }

        flushAndCloseCsvFile();

        headerFileWriter.startWriting();
        postWritingPhaseManager.notifyWritingHasFinished();

        return;
    }

    private void writeRows() throws IOException {

        try {
            DataGetter dataGetter = fileDataQueue.poll(5, TimeUnit.SECONDS);
            if (dataGetter == null)
                return;

            printWholeData(dataGetter);
            printer.flush();
            System.gc();
        } catch (InterruptedException e) {
        }

        return;
    }

    private void printWholeData(DataGetter dataGetter) throws IOException {

        int chances = 10;
        dataGetter.readFromBeginning();
        String sourceNameToWriteOnRows = getRepresentationOnFirstRow(dataGetter.getSourceName());

        while (true) {
            float[] dataRow;

            try {
                dataRow = dataGetter.getNextRow();
            } catch (NoSuchElementException e) {
                break;
            }

            int count = 0;
            while (true) {
                try {
                    List<Object> completeRow = new ArrayList<>(dataRow.length);
                    if (putFileNameAsFirstColumn)
                        completeRow.add(sourceNameToWriteOnRows);
                    for (float singleData : dataRow)
                        completeRow.add(singleData);
                    printer.printRecord(completeRow);
                    break;
                } catch (IOException e) {
                    count++;
                    if (count == chances)
                        throw e;
                }
            }
        }

    }

    private String getRepresentationOnFirstRow(String sourceFullName) {
        String fileName = new File(sourceFullName).getName();
        try {
            return fileName.substring(fileName.indexOf(".") + 1, fileName.lastIndexOf("."));
        } catch (IndexOutOfBoundsException e) {
            return fileName.substring(0, fileName.lastIndexOf("."));
        }
    }

    public void addDataGetter(DataGetter dataGetter) {

        try {
            fileDataQueue.put(dataGetter);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return;
    }

    private void openCsvFile() throws IOException {

        infoLogger.log(String.format(NEW_FILE_CREATED_MSG, csvFileFullPath),
                NEW_FILE_CREATED_LOG_LVL);

        String[] columnNames;

        if (putFileNameAsFirstColumn) {
            columnNames = new String[columnNamesForData.length + 1];
            columnNames[0] = nameOfColumnForSourcesNames;
            System.arraycopy(columnNamesForData, 0, columnNames, 1, columnNamesForData.length);
        } else
            columnNames = columnNamesForData;

        printer = CSVFormat.DEFAULT.withHeader(columnNames).print(Files.newBufferedWriter(
                Paths.get(csvFileFullPath)
        ));

        return;
    }

    private void flushAndCloseCsvFile() {

        try {
            printer.flush();
        } catch (IOException e) {
            infoLogger.log(String.format(COULDNT_WRITE_ON_CSV_MSG, csvFileFullPath),
                    COULDNT_WRITE_ON_CSV_LOG_LVL);
        }

        try {
            printer.close();
        } catch (IOException e) {
            infoLogger.log(String.format(COULDNT_CLOSE_CSV_MSG, csvFileFullPath),
                    COULDNT_CLOSE_CSV_LOG_LVL);
        }

    }

    public void sourceHasFinished(String fileName) {
        sourceFiles.remove(fileName);
        return;
    }

}
