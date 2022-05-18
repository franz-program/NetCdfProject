import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/*TODO: da progettare bene il discorso chiusura risorse, perchè alcune vanno chiuse quando termino tutto
alcune vanno chiuse come operazione standard, altre sono chiuse quando avvengono errori fatali
*/

//TODO: quando faccio log stampo anche messaggio eccezzione?

//TODO: da vedere se dò troppi callable/runnable all'executorService facendolo crashare

public class NetcdfDataAggregatorV1 implements NetcdfDataAggregator {

    private final NetcdfRowsManager asynchronousTableWriter;
    private final InfoLogger infoLogger;
    private AsynchronousFailedFilesManager asynchronousFailedFilesManager;
    private AsynchronousHeaderFileWriter asynchronousHeaderFileWriter;

    private List<String> netcdfFilesNames;
    private List<String> failedPreprocessNetcdfFiles = new ArrayList<>();

    private ExecutorService executorService;

    private final static String FILE_WONT_BE_PROCESSED_MSG = "The file %s will not be processed";
    private final static LogLevel FILE_WONT_BE_PROCESSED_LOG_LVL = LogLevel.ERROR;

    public NetcdfDataAggregatorV1(NetcdfRowsManager asynchronousTableWriter, InfoLogger infoLogger,
                                  List<String> netcdfFilesNames, AsynchronousFailedFilesManager asynchronousFailedFilesManager,
                                  AsynchronousHeaderFileWriter asynchronousHeaderFileWriter,
                                  ExecutorService executorService) {

        this.asynchronousTableWriter = asynchronousTableWriter;
        this.infoLogger = infoLogger;
        this.netcdfFilesNames = netcdfFilesNames;
        this.asynchronousFailedFilesManager = asynchronousFailedFilesManager;
        this.asynchronousHeaderFileWriter = asynchronousHeaderFileWriter;
        this.executorService = executorService;

    }

    @Override
    public void aggregate() {

        filterInvalidFiles();

        List<ColumnInfo> dependentColumnsInfo = getDependentColumnsInfo();
        asynchronousHeaderFileWriter.setColumnsInfo(dependentColumnsInfo);
        new Thread(asynchronousHeaderFileWriter).start();


        asynchronousFailedFilesManager.setFailedFilesNames(failedPreprocessNetcdfFiles);
        new Thread(asynchronousFailedFilesManager).start();


        String[] columnNames = getColumnNames(dependentColumnsInfo);

        asynchronousTableWriter.setColumnNames(columnNames);
        asynchronousTableWriter.setSourceNames(netcdfFilesNames);
        new Thread(asynchronousTableWriter).start();

        startReadingNetcdfFiles(columnNames);

    }

    private void filterInvalidFiles() {

        Map<String, Future<Boolean>> futureMap = createFutureMapForFirstCheck();

        try {

            for (Map.Entry<String, Future<Boolean>> pair : futureMap.entrySet()) {
                try {
                    processTestedFile(pair.getValue().get(), pair.getKey());
                } catch (ExecutionException e) {
                    processTestedFile(false, pair.getKey());
                }
            }

        } catch (InterruptedException e) {
            infoLogger.log(e.toString(), LogLevel.ERROR);
            //TODO: quando avviene?
            //TODO: cosa faccio?
        }
    }

    private Map<String, Future<Boolean>> createFutureMapForFirstCheck() {
        //TODO: qual è la migliore implementazione?
        Map<String, Future<Boolean>> map = new HashMap<>();

        for (String netcdfFileName : netcdfFilesNames)
            map.put(netcdfFileName, executorService.submit(NetcdfFileCheckerCreator.create(netcdfFileName, infoLogger)));

        return map;
    }

    private void processTestedFile(boolean testResult, String fileFullPath) {
        if (!testResult) {
            netcdfFilesNames.remove(fileFullPath);
            infoLogger.log(String.format(FILE_WONT_BE_PROCESSED_MSG, fileFullPath), FILE_WONT_BE_PROCESSED_LOG_LVL);
            failedPreprocessNetcdfFiles.add(fileFullPath);
        }
    }

    private List<ColumnInfo> getDependentColumnsInfo() {

        Map<String, Future<List<ColumnInfo>>> futureMap = createFutureMapForColumnInfo();
        List<ColumnInfo> globalColumnsInfo = new ArrayList<>();

        try {
            for (Map.Entry<String, Future<List<ColumnInfo>>> pair : futureMap.entrySet()) {
                try {
                    addElementsIfNotPresent(globalColumnsInfo, pair.getValue().get());
                } catch (ExecutionException e) {
                    //TODO: occhio ripetizione di codice
                    String fileFullPath = pair.getKey();
                    netcdfFilesNames.remove(fileFullPath);
                    infoLogger.log(String.format(FILE_WONT_BE_PROCESSED_MSG, fileFullPath), FILE_WONT_BE_PROCESSED_LOG_LVL);
                    failedPreprocessNetcdfFiles.add(fileFullPath);
                }
            }
        } catch (InterruptedException e) {
            infoLogger.log(e.toString(), LogLevel.ERROR);
            //TODO: quando avviene?
            //TODO: cosa faccio?
        }

        return globalColumnsInfo;

    }

    private Map<String, Future<List<ColumnInfo>>> createFutureMapForColumnInfo() {
        //TODO: qual è la migliore implementazione?
        Map<String, Future<List<ColumnInfo>>> map = new HashMap<>();

        for (String netcdfFileName : netcdfFilesNames)
            map.put(netcdfFileName, executorService.submit(DepVariablesInfoGetterCreator.create(netcdfFileName, infoLogger)));

        return map;
    }

    private void addElementsIfNotPresent(List<ColumnInfo> listWhereToAdd, List<ColumnInfo> listToCheck) {
        for (ColumnInfo columnInfo : listToCheck) {
            if (!listWhereToAdd.contains(columnInfo))
                listWhereToAdd.add(columnInfo);
        }
    }

    private String[] getColumnNames(List<ColumnInfo> dependentColumnsInfo) {

        String[] depNames = dependentColumnsInfo.stream().map(ColumnInfo::getName).toArray(String[]::new);
        String[] columnNames = new String[4 + depNames.length];
        columnNames[0] = ClassForCostants.timeVarName;
        columnNames[1] = ClassForCostants.depthVarName;
        columnNames[2] = ClassForCostants.latVarName;
        columnNames[3] = ClassForCostants.lonVarName;

        for (int i = 4; i < columnNames.length; i++) {
            columnNames[i] = depNames[i - 4];
        }
        return columnNames;
    }

    private void startReadingNetcdfFiles(String[] columnNames) {

        for (String fileName : netcdfFilesNames)
            executorService.execute(NetcdfRowsSenderCreator.create(asynchronousTableWriter, columnNames, fileName,
                    infoLogger));

        terminateExecutorService();
    }

    private void terminateExecutorService() {
        if (executorService != null)
            executorService.shutdown();
    }

}
