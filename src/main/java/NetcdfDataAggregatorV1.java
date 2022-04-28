import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/*TODO: da progettare bene il discorso chiusura risorse, perchè alcune vanno chiuse quando termino tutto
alcune vanno chiuse come operazione standard, altre sono chiuse quando avvengono errori fatali
*/

//TODO: quando faccio log stampo anche messaggio eccezzione?


public class NetcdfDataAggregatorV1 implements NetcdfDataAggregator {

    private final AsynchronousTableWriter asynchronousTableWriter;
    private final InfoLogger infoLogger;

    private String[] netcdfFilesNames;
    private final String headerFileName = "header.csv";

    private ExecutorService executorService;
    private final int nOfActiveThreads = Runtime.getRuntime().availableProcessors();


    private final List<NetcdfFile> netcdfFiles = new ArrayList<>();

    //VARIABLES FOR MESSAGES

    private final String UNABLE_TO_OPEN_FILE_MSG = "The program was unable to open %s, shutting everything down";
    private final LogLevel UNABLE_TO_OPEN_FILE_LOG_LVL = LogLevel.ERROR;

    private final String UNABLE_TO_CLOSE_FILE_MSG = "The program was unable to close %s, shutting everything down";
    private final LogLevel UNABLE_TO_CLOSE_FILE_LOG_LVL = LogLevel.ERROR;


    public NetcdfDataAggregatorV1(AsynchronousTableWriter asynchronousTableWriter, InfoLogger infoLogger,
                                  String[] netcdfFilesNames) {
        this.asynchronousTableWriter = asynchronousTableWriter;
        this.infoLogger = infoLogger;

        this.netcdfFilesNames = netcdfFilesNames;

        openNetcdfFiles();
    }


    @Override
    public void aggregate() {

        performFirstCheck();

        executorService = Executors.newFixedThreadPool(nOfActiveThreads);
        List<Callable<List<ColumnInfo>>> callablesForColumnInfo = createCallablesForColumnInfo();
        List<ColumnInfo> dependentColumnsInfo = getDependentColumnsInfo(callablesForColumnInfo);
        String[] columnNames = getColumnNames(dependentColumnsInfo);
        terminateExecutorService();

        asynchronousTableWriter.setColumnNames(columnNames);
        asynchronousTableWriter.startTask();


        completeProgram();
    }

    private void openNetcdfFiles() {
        for (String fileName : netcdfFilesNames) {
            try {
                netcdfFiles.add(NetcdfFiles.open(fileName));
            } catch (IOException e) {
                infoLogger.log(String.format(UNABLE_TO_OPEN_FILE_MSG, fileName), UNABLE_TO_OPEN_FILE_LOG_LVL);
                //TODO: chiudo tutto?
            }
        }
    }


    private void completeProgram() {
        closeResources();
    }


    private void closeResources() {
        closeNetcdfFiles();
        terminateExecutorService();

        //meglio chiudere il logger per ultimo
        infoLogger.close();
        System.exit(0);
        //TODO: va bene system exit per fermare il thread?
    }


    private void closeNetcdfFiles() {
        for (NetcdfFile netcdfFile : netcdfFiles) {
            try {
                netcdfFile.close();
            } catch (IOException e) {
                infoLogger.log(String.format(UNABLE_TO_CLOSE_FILE_MSG, netcdfFile.getLocation())
                        , UNABLE_TO_CLOSE_FILE_LOG_LVL);
                //TODO: cosa faccio? il metodo lo chiamo solo da closeResources()? Se si, allora non faccio nulla in questo catch?
            }
        }
    }


    //TODO: vedi come fare per executorService

    private void terminateExecutorService() {
        if (executorService != null)
            executorService.shutdown();
    }

    private List<Callable<Boolean>> createCallablesForFirstCheck() {
        List<Callable<Boolean>> callables = new ArrayList<>();

        for (NetcdfFile netcdfFile : netcdfFiles)
            callables.add(NetcdfFileCheckerCreator.createFileChecker(netcdfFile, infoLogger));

        return callables;
    }

    private List<Callable<List<ColumnInfo>>> createCallablesForColumnInfo() {
        List<Callable<List<ColumnInfo>>> callables = new ArrayList<>();

        for (NetcdfFile netcdfFile : netcdfFiles)
            callables.add(DepVariablesInfoGetterCreator.createVariablesInfoGetter(netcdfFile));

        return callables;
    }

    private void performFirstCheck() {
        boolean firstCheckPassed = true;
        List<Callable<Boolean>> callables = createCallablesForFirstCheck();
        executorService = Executors.newFixedThreadPool(nOfActiveThreads);

        //TODO: metto uno sleep usando future.isDone() invece che get?
        //TODO: da capire cosa causa InterruptedException

        try {
            List<Future<Boolean>> futures = executorService.invokeAll(callables);
            for (Future<Boolean> future : futures)
                if (!future.get())
                    firstCheckPassed = false;

        } catch (InterruptedException | ExecutionException e) {
            infoLogger.log(e.toString(), LogLevel.ERROR);
            firstCheckPassed = false;
        }

        if (!firstCheckPassed) {
            closeResources();
        }

        terminateExecutorService();

    }

    private List<ColumnInfo> getDependentColumnsInfo(List<Callable<List<ColumnInfo>>> callables) {
        List<ColumnInfo> columnsInfo = new ArrayList<>();

        try {
            List<Future<List<ColumnInfo>>> futures = executorService.invokeAll(callables);
            for (Future<List<ColumnInfo>> future : futures) {
                for (ColumnInfo columnInfo : future.get())
                    if (!columnsInfo.contains(columnInfo))
                        columnsInfo.add(columnInfo);
            }
        } catch (InterruptedException | ExecutionException e) {
            //TODO:

        }
        return columnsInfo;
    }

    private String[] getColumnNames(List<ColumnInfo> dependentColumnsInfo) {

        String[] depNames = dependentColumnsInfo.stream().map(ColumnInfo::getName).toArray(String[]::new);
        String[] columnNames = new String[4 + depNames.length];
        columnNames[0] = ClassForCostants.timeVarName;
        columnNames[1] = ClassForCostants.depthVarName;
        columnNames[2] = ClassForCostants.latVarName;
        columnNames[3] = ClassForCostants.lonVarName;

        for (int i = 4; i < depNames.length; i++) {
            columnNames[i] = depNames[i - 4];
        }
        return columnNames;
    }


}
