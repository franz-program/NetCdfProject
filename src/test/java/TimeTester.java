import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TimeTester implements PostWritingPhaseManager {

    private static String configFilePathForInputFiles;
    private static String configFilePathForWantedVarNames;
    private static String outputFileFullPath;
    private static String headerFileFullPath;
    private static String logFileFullPath;


    private static final int nOfTestsPerPoolSize = 3;

    public static void main(String[] args) {

        System.out.println("Remember to set the max heap size");

        if (args.length < 5) {
            System.out.printf("ARGUMENTS REQUIRED: %n0) path of txt file containing paths of netcdf files (one per row)%n");
            System.out.printf("1) path of txt file containing netcdf var names%n2) any path for the output file (csv)%n");
            System.out.printf("3) any path for header file (csv)%n4) any path for log file (txt)%n");
            return;
        }

        configFilePathForInputFiles = args[0];
        configFilePathForWantedVarNames = args[1];
        outputFileFullPath = args[2];
        headerFileFullPath = args[3];
        logFileFullPath = args[4];

        int maxNOfThreads = 10;

        for (int i = 1; i <= maxNOfThreads; i++)
            processTimeTestWithFixedPoolSize(i);

    }

    private static void processTimeTestWithFixedPoolSize(int poolSize) {

        long sum = 0;

        for (int i = 0; i < nOfTestsPerPoolSize; i++) {
            ExecutorService executorService = Executors.newFixedThreadPool(poolSize);
            PostWritingPhaseManager controllerIfSwHasFinished = new TimeTester(executorService);
            long startingTime = System.currentTimeMillis();

            Main.externalPostWritingPhaseManager = controllerIfSwHasFinished;
            Main.externalExecutorService = executorService;
            Main.main(new String[]{configFilePathForInputFiles, outputFileFullPath,
                    headerFileFullPath, configFilePathForWantedVarNames, logFileFullPath});

            synchronized (controllerIfSwHasFinished) {
                try {
                    controllerIfSwHasFinished.wait();
                } catch (InterruptedException e) {
                }
            }
            sum += System.currentTimeMillis() - startingTime;
        }


        int avgTimeInS = (int) ((double) sum / (1000.0 * nOfTestsPerPoolSize));

        System.out.printf("Pool size %d, avg time in seconds: %d%n", poolSize, avgTimeInS);

    }


    private final ExecutorService executorService;

    public TimeTester(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public void notifyWritingHasFinished() {
        executorService.shutdown();
        executorService.shutdownNow();
        synchronized (this) {
            this.notify();
        }
    }

}
