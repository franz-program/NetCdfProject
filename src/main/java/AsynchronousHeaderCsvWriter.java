import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class AsynchronousHeaderCsvWriter implements AsynchronousHeaderFileWriter {

    private List<ColumnInfo> columnsInfo;

    private InfoLogger infoLogger;
    private String headerFileFullPath;
    private CSVPrinter printer;

    private final String NO_COLUMN_INFO_SET_MSG = "No column info was set for the header file writer";
    private final LogLevel NO_COLUMN_INFO_SET_LOG_LVL = LogLevel.ERROR;

    private final String START_WRITING_HEADER_FILE_MSG = "Started writing the header file %s";
    private final LogLevel START_WRITING_HEADER_FILE_LOG_LVL = LogLevel.NORMAL;

    private final String WASNT_ABLE_TO_WORK_ON_CSV_FILE_MSG = "The program wasn't able to work on csv header file %s";
    private final LogLevel WASNT_ABLE_TO_WORK_ON_CSV_FILE_LOG_LVL = LogLevel.ERROR;

    private final String COULDNT_WRITE_ON_CSV_MSG = "The program tried to write in header file %s but failed";
    private final LogLevel COULDNT_WRITE_ON_CSV_LOG_LVL = LogLevel.ERROR;

    private final String COULDNT_CLOSE_CSV_MSG = "The program tried to close header file %s but failed";
    private final LogLevel COULDNT_CLOSE_CSV_LOG_LVL = LogLevel.ERROR;

    private final String PRINT_ON_LOG_MSG = "%n********************%n***************" +
            "The program will print header file on log" +
            "%n*******************%n**************";
    private final LogLevel PRINT_ON_LOG_LOG_LVL = LogLevel.WARNING;


    public AsynchronousHeaderCsvWriter(InfoLogger infoLogger, String headerFileFullPath) {
        this.infoLogger = infoLogger;
        this.headerFileFullPath = headerFileFullPath + (headerFileFullPath.endsWith(".csv") ? "" : ".csv");
    }

    @Override
    public void setColumnsInfo(List<ColumnInfo> columnsInfo) {
        this.columnsInfo = columnsInfo;
    }

    public void run() {

        if (Objects.isNull(columnsInfo)) {
            infoLogger.log(NO_COLUMN_INFO_SET_MSG, NO_COLUMN_INFO_SET_LOG_LVL);
            return;
        }

        try {
            printer = CSVFormat.DEFAULT.withHeader(ColumnInfo.getKindOfInfo()).print(Files.newBufferedWriter(
                    Paths.get(headerFileFullPath)
            ));
            infoLogger.log(String.format(START_WRITING_HEADER_FILE_MSG, headerFileFullPath), START_WRITING_HEADER_FILE_LOG_LVL);
            normalPrinting();
            flushAndCloseCsvFile();
        } catch (IOException e) {
            infoLogger.log(String.format(WASNT_ABLE_TO_WORK_ON_CSV_FILE_MSG, headerFileFullPath),
                    WASNT_ABLE_TO_WORK_ON_CSV_FILE_LOG_LVL);
            printOnLog();
        }

    }

    private void normalPrinting() throws IOException {
        try {
            for (ColumnInfo columnInfo : columnsInfo)
                printer.printRecord(columnInfo.getFullInfo());
        } catch (IOException e) {
            infoLogger.log(String.format(COULDNT_WRITE_ON_CSV_MSG, headerFileFullPath), COULDNT_WRITE_ON_CSV_LOG_LVL);
            throw e;
        }
    }

    private void flushAndCloseCsvFile() throws IOException {

        try {
            printer.flush();
        } catch (IOException e) {
            infoLogger.log(String.format(COULDNT_WRITE_ON_CSV_MSG, headerFileFullPath),
                    COULDNT_WRITE_ON_CSV_LOG_LVL);
            throw e;
        }

        try {
            printer.close();
        } catch (IOException e) {
            infoLogger.log(String.format(COULDNT_CLOSE_CSV_MSG, headerFileFullPath),
                    COULDNT_CLOSE_CSV_LOG_LVL);
            throw e;
            //TODO: lascio aperto?
        }

    }

    private void printOnLog() {
        infoLogger.log(PRINT_ON_LOG_MSG, PRINT_ON_LOG_LOG_LVL);
        infoLogger.log(getColumnsInfoStringRepresentation(), LogLevel.ERROR);
    }

    private String getColumnsInfoStringRepresentation() {

        String representation = "";
        for (ColumnInfo columnInfo : columnsInfo)
            representation += trimSquareBrackets(Arrays.toString(columnInfo.getFullInfo())) + "%n";

        return representation;
    }

    private String trimSquareBrackets(String s) {

        if (s.indexOf("[") == 0)
            s = s.substring(1);

        if (s.lastIndexOf("]") == s.length() - 1)
            s = s.substring(0, s.length() - 1);

        return s;
    }


}
