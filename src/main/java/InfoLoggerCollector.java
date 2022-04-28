import java.util.ArrayList;
import java.util.List;

public class InfoLoggerCollector implements InfoLogger{

    private List<InfoLogger> loggers;

    public InfoLoggerCollector(List<InfoLogger> loggers){
        this.loggers = loggers;
    }

    public void log(String msg, LogLevel level){

        for(InfoLogger logger: loggers)
            logger.log(msg, level);

    }

    public void close(){
        for(InfoLogger logger: loggers)
            logger.close();
    }



}
