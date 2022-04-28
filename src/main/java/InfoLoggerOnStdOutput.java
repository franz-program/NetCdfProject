public class InfoLoggerOnStdOutput implements InfoLogger{

    private LogLevel minimumLoggingLevel;
    private final String STANDARD_LOG_FORMAT = "At %s %s: %s%n";

    public InfoLoggerOnStdOutput(LogLevel minimumLoggingLevel){
        this.minimumLoggingLevel = minimumLoggingLevel;
    }

    public void log(String msg, LogLevel level){
        if(level.isHigherOrEqualLevel(minimumLoggingLevel))
            System.err.printf(STANDARD_LOG_FORMAT, java.time.LocalDate.now(), level.toString(), msg);

        return;
    }

    public void close(){
        return;
    }

}
