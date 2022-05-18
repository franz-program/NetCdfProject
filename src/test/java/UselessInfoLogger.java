public class UselessInfoLogger implements InfoLogger{

    public UselessInfoLogger(){
    }

    @Override
    public void log(String msg, LogLevel level) {
        return;
    }

    @Override
    public void close() {
        return;
    }
}
