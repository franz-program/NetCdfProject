import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    //TODO: aggiorna gli indirizzi hardcoded
    //TODO: aggiungi un blocco del programma nel main se alcune ipotesi fondamentali falliscono

    public static void main(String[] args) {

        System.out.println("Remember to set correctly the maxHeapSize");

        List<String> ncFilesPaths;
        String outputFileFullPath;
        String headerFileFullPath;
        String[] variablesNames;

        ncFilesPaths = getNcFilesPaths(args.length < 1 ? getConfigFileFullPath() : args[0]);
        outputFileFullPath = args.length < 2 ? getOutputFileFullPath() : args[1];
        headerFileFullPath = args.length < 3 ? getHeaderFileFullPath() : args[2];
        variablesNames = getVariablesNames(args.length < 4 ? get4DNamesConfigFileFullPath() : args[3]);

        InfoLogger logCollector = new InfoLoggerCollector(instantiateLoggers());

        HeaderFileWriter headerFileWriter = new HeaderCsvWriter(logCollector, headerFileFullPath);


        int nOfActiveThreads = PoolSizeCalculator.getPoolSize(ncFilesPaths, logCollector);
        nOfActiveThreads = Math.min(nOfActiveThreads, ncFilesPaths.size());
        //nOfActiveThreads = 1;
        ExecutorService executorService = Executors.newFixedThreadPool(nOfActiveThreads);

        //TODO: VA BENE?
        Thread temporaryPostWritingPhaseManager = new ResourceCloser(logCollector, executorService);
        temporaryPostWritingPhaseManager.start();
        PostWritingPhaseManager postWritingPhaseManager = (PostWritingPhaseManager) temporaryPostWritingPhaseManager;
        //

        AsynchronousOutputFileWriter outputFileWriter = new CsvWriterFromData(logCollector, outputFileFullPath,
                postWritingPhaseManager, ncFilesPaths, variablesNames,
                headerFileWriter, true);
        new Thread(outputFileWriter).start();

        AsynchronousFailedFilesManager asynchronousFailedFilesManager = new FailedFilesPrinter(logCollector, outputFileWriter);
        new Thread(asynchronousFailedFilesManager).start();

        AsynchronousFilesValidityCollector asynchronousFilesValidityCollector = new InvalidFilesPrinter(logCollector, outputFileWriter);
        new Thread(asynchronousFilesValidityCollector).start();

        startExecutingReaders(ncFilesPaths, executorService, outputFileWriter, variablesNames,
                logCollector, asynchronousFilesValidityCollector, asynchronousFailedFilesManager, headerFileWriter);

    }

    private static InfoLogger[] instantiateLoggers() {

        InfoLogger[] loggers = new InfoLogger[2];
        loggers[0] = new InfoLoggerOnTxtFile(getLogFileFullPath());
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

    private static String[] getVariablesNames(String filePathContaining4DVarNames) {
        String[] indepVarsNames = new String[]{ClassForCostants.depthVarName,
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

    private static void startExecutingReaders(List<String> fileFullPaths, ExecutorService executorService, AsynchronousOutputFileWriter outputFileWriter,
                                              String[] columnsNames, InfoLogger infoLogger,
                                              AsynchronousFilesValidityCollector filesValidityCollector,
                                              AsynchronousFailedFilesManager failedFilesManager, HeaderFileWriter headerFileWriter) {

        for (String filePath : fileFullPaths)
            executorService.execute(NetcdfDataReaderCreator.create(outputFileWriter, columnsNames, filePath, infoLogger,
                    filesValidityCollector, failedFilesManager, headerFileWriter));

    }

}
