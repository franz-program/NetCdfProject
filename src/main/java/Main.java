import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    //TODO: aggiorna gli indirizzi hardcoded
    //TODO: aggiungi un blocco del programma nel main se alcune ipotesi fondamentali falliscono

    public static int nOfActiveThreads = Runtime.getRuntime().availableProcessors() - 1;

    public static ExecutorService externalExecutorService = null;
    public static PostWritingPhaseManager externalPostWritingPhaseManager = null;

    public static void main(String[] args) {

        List<String> ncFilesPaths;
        String outputFileFullPath;
        String headerFileFullPath;
        String[] variablesNames;
        String logFileFullPath;

        ncFilesPaths = getNcFilesPaths(args.length < 1 ? getConfigFileFullPath() : args[0]);
        outputFileFullPath = args.length < 2 ? getOutputFileFullPath() : args[1];
        headerFileFullPath = args.length < 3 ? getHeaderFileFullPath() : args[2];
        variablesNames = getVariablesNames(args.length < 4 ? get4DNamesConfigFileFullPath() : args[3]);
        logFileFullPath = args.length < 5 ? getLogFileFullPath() : args[4];

        InfoLogger logCollector = new InfoLoggerCollector(instantiateLoggers(logFileFullPath));

        AsynchronousHeaderFileWriter asynchronousHeaderFileWriter = new AsynchronousHeaderCsvWriter(logCollector,
                headerFileFullPath);

        ExecutorService executorService;
        executorService = (externalExecutorService == null) ? Executors.newFixedThreadPool(nOfActiveThreads) : externalExecutorService;


        //TODO: VA BENE?
        PostWritingPhaseManager postWritingPhaseManager;
        if (externalPostWritingPhaseManager == null) {
            Thread temporaryPostWritingPhaseManager = new ResourceCloser(logCollector, executorService);
            temporaryPostWritingPhaseManager.start();
            postWritingPhaseManager = (PostWritingPhaseManager) temporaryPostWritingPhaseManager;
        } else {
            postWritingPhaseManager = externalPostWritingPhaseManager;
        }


        AsynchronousFailedFilesManager asynchronousFailedFilesManager = new FailedFilesPrinter(logCollector);

        NetcdfRowsManager asynchronousTableWriter = new CsvWriterFromMultipleFiles(logCollector, outputFileFullPath,
                postWritingPhaseManager);


        NetcdfDataAggregator netcdfDataAggregator = new NetcdfDataAggregatorV1(asynchronousTableWriter, logCollector,
                ncFilesPaths, asynchronousFailedFilesManager, asynchronousHeaderFileWriter,
                executorService, variablesNames);

        netcdfDataAggregator.aggregate();
    }

    private static InfoLogger[] instantiateLoggers(String logFileFullPath) {

        InfoLogger[] loggers = new InfoLogger[2];
        loggers[0] = new InfoLoggerOnTxtFile(logFileFullPath);
        loggers[1] = new InfoLoggerOnStdOutput(LogLevel.WARNING);

        return loggers;
    }

    private static String getLogFileFullPath() {
        return "D:\\UNIVERSITA'\\TESI\\generated files folder\\logFileName.txt";
    }

    private static String getConfigFileFullPath() {
        return "D:\\UNIVERSITA'\\TESI\\config files\\netcdfListConfigFile.txt";
    }

    private static String getHeaderFileFullPath() {
        return "D:\\UNIVERSITA'\\TESI\\generated files folder\\headerFile.csv";
    }

    private static String getOutputFileFullPath() {
        return "D:\\UNIVERSITA'\\TESI\\generated files folder\\outputFileName.csv";
    }

    private static String get4DNamesConfigFileFullPath() {
        return "D:\\UNIVERSITA'\\TESI\\config files\\4DVariablesNames.txt";
    }

    private static List<String> getNcFilesPaths(String configFileFullPath) {

        BufferedReader bufferedReader;
        List<String> fileNames = new ArrayList<>();

        try {
            bufferedReader = new BufferedReader(new FileReader(configFileFullPath));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Config file not found");
        }


        String line;
        while (true) {

            try {
                line = bufferedReader.readLine();
            } catch (IOException e) {
                throw new RuntimeException("Could not read config file");
            }

            if (Objects.isNull(line))
                break;

            if (new File(line).isDirectory())
                getNcFilesFromFolder(line, fileNames);
            else if (line.endsWith(".nc") && new File(line).isFile()) {
                if (!fileNames.contains(line))
                    fileNames.add(line);
            }
        }
        return fileNames;
    }

    private static String[] getVariablesNames(String filePathContaining4DVarNames) {
        String[] indepVarsNames = new String[]{ClassForCostants.timeVarName, ClassForCostants.depthVarName,
                ClassForCostants.latVarName, ClassForCostants.lonVarName};
        String[] depVarsNames = get4DVariablesNamesFromTxtFile(filePathContaining4DVarNames);

        String[] varsNames = new String[indepVarsNames.length + depVarsNames.length];
        System.arraycopy(indepVarsNames, 0, varsNames, 0, indepVarsNames.length);
        System.arraycopy(depVarsNames, 0, varsNames, indepVarsNames.length, depVarsNames.length);

        return varsNames;
    }

    private static String[] get4DVariablesNamesFromTxtFile(String filePathContaining4DVarNames) {
        BufferedReader bufferedReader;
        List<String> varNames = new ArrayList<>();

        try {
            bufferedReader = new BufferedReader(new FileReader(filePathContaining4DVarNames));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File containing 4D variables names not found");
        }

        String line;
        while (true) {

            try {
                line = bufferedReader.readLine();
            } catch (IOException e) {
                throw new RuntimeException("Could not read 4D var names file");
            }

            if (Objects.isNull(line))
                break;

            line = line.trim();

            if (line.length() == 0)
                continue;

            varNames.add(line);
        }

        return varNames.toArray(String[]::new);
    }

    private static List<String> getNcFilesNames(String configFileFullPath) {

        BufferedReader bufferedReader;
        List<String> fileNames = new ArrayList<>();

        try {
            bufferedReader = new BufferedReader(new FileReader(configFileFullPath));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Config file not found");
        }


        String line;
        while (true) {
            try {
                line = bufferedReader.readLine();
            } catch (IOException e) {
                throw new RuntimeException("Could not read file");
            }

            if (Objects.isNull(line))
                break;

            if (new File(line).isDirectory())
                getNcFilesFromFolder(line, fileNames);
            else if (line.endsWith(".nc") && new File(line).isFile()) {
                if (!fileNames.contains(line))
                    fileNames.add(line);
            }
        }
        return fileNames;
    }

    private static void getNcFilesFromFolder(String folderPath, List<String> currentAddedFiles) {
        File folder = new File(folderPath);
        for (String file : folder.list()) {
            if (new File(folderPath + File.separator + file).isDirectory())
                getNcFilesFromFolder(folderPath + File.separator + file, currentAddedFiles);
            else if (file.endsWith(".nc")) {
                if (!currentAddedFiles.contains(folderPath + File.separator + file))
                    currentAddedFiles.add(folderPath + File.separator + file);
            }
        }
        return;
    }


}
