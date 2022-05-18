import java.util.List;

public interface AsynchronousFailedFilesManager extends Runnable{

    void setFailedFilesNames(List<String> filesNames);

}
