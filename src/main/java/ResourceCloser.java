import java.util.concurrent.ExecutorService;

public class ResourceCloser extends Thread implements PostWritingPhaseManager {

    private InfoLogger infoLogger;
    private ExecutorService executorService;

    public ResourceCloser(InfoLogger infoLogger, ExecutorService executorService) {
        this.infoLogger = infoLogger;
        this.executorService = executorService;
    }

    @Override
    public void notifyWritingHasFinished() {
        synchronized (this) {
            this.notify();
        }
    }


    @Override
    public void run() {

        try {
            synchronized (this) {
                this.wait();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            closeResources();
        }

        return;
    }

    private void closeResources() {
        killExecutorServiceAndTasks();
        infoLogger.close();
    }

    private void killExecutorServiceAndTasks() {
        executorService.shutdown();
    }


}
