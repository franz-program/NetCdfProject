import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TimeTester implements PostWritingPhaseManager {

    private static final String configFilePathForInputFiles = null;
    private static final String configFilePathForWantedVarNames = null;
    private static final String outputFileFullPath = null;
    private static final String headerFileFullPath = null;


    private static final int nOfTestsPerPoolSize = 5;


    public static void main(String[] args) {


        int maxNOfThreads = Runtime.getRuntime().availableProcessors() - 1;

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
            Main.main(new String[]{configFilePathForInputFiles, configFilePathForWantedVarNames,
                    outputFileFullPath, headerFileFullPath});

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
