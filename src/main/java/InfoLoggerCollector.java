public class InfoLoggerCollector implements InfoLogger{

    private InfoLogger[] loggers;

    public InfoLoggerCollector(InfoLogger[] loggers){
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
