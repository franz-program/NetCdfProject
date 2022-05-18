import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    //TODO: aggiorna gli indirizzi hardcoded
    //TODO: aggiungi un blocco del programma nel main se alcune ipotesi fondamentali falliscono

    private final static int nOfActiveThreads = Runtime.getRuntime().availableProcessors() - 1;

    public static void main(String[] args) {

        List<String> ncFileNames;
        String outputFileFullPath;
        String headerFileFullPath;

        if (args.length < 1)
            ncFileNames = getNcFilesNames("D:\\UNIVERSITA'\\TESI\\configFile.txt");
        else
            ncFileNames = getNcFilesNames(args[0]);

        if(args.length < 2)
            outputFileFullPath = getOutputFileFullPath();
        else
            outputFileFullPath = args[1];

        if(args.length < 3)
            headerFileFullPath = getHeaderFileFullPath();
        else
            headerFileFullPath = args[2];


        InfoLogger logCollector = new InfoLoggerCollector(instantiateLoggers());

        AsynchronousHeaderFileWriter asynchronousHeaderFileWriter = new AsynchronousHeaderCsvWriter(logCollector,
                headerFileFullPath);

        ExecutorService executorService = Executors.newFixedThreadPool(nOfActiveThreads);

        //TODO: VA BENE?
        Thread temporaryPostWritingPhaseManager = new ResourceCloser(logCollector, executorService);
        temporaryPostWritingPhaseManager.start();
        PostWritingPhaseManager postWritingPhaseManager = (PostWritingPhaseManager) temporaryPostWritingPhaseManager;
        //


        AsynchronousFailedFilesManager asynchronousFailedFilesManager = new FailedFilesPrinter(logCollector);

        NetcdfRowsManager asynchronousTableWriter = new CsvWriterFromMultipleFiles(logCollector, outputFileFullPath,
                postWritingPhaseManager);


        NetcdfDataAggregator netcdfDataAggregator = new NetcdfDataAggregatorV1(asynchronousTableWriter, logCollector,
                ncFileNames, asynchronousFailedFilesManager, asynchronousHeaderFileWriter,
                executorService);

        netcdfDataAggregator.aggregate();
    }

    private static InfoLogger[] instantiateLoggers() {

        InfoLogger[] loggers = new InfoLogger[2];
        String[] logFileInfo = getLogFileFolderAndName();
        loggers[0] = new InfoLoggerOnTxtFile(logFileInfo[0], logFileInfo[1]);
        loggers[1] = new InfoLoggerOnStdOutput(LogLevel.WARNING);

        return loggers;
    }

    private static String[] getLogFileFolderAndName() {
        //folder = pos 0
        //name = pos 1
        return new String[]{"D:\\UNIVERSITA'\\TESI\\other files folder", "logFileName.txt"};
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

    private static String getHeaderFileFullPath() {
        return "D:\\UNIVERSITA'\\TESI\\other files folder\\headerFile.csv";
    }

    private static String getOutputFileFullPath() {
        return "D:\\UNIVERSITA'\\TESI\\output file folder\\outputFileName.csv";
    }


}
