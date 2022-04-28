public interface InfoLogger {

    void log(String msg, LogLevel level) ;

    void close();

}
