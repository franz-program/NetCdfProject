public enum LogLevel {
    NORMAL,
    WARNING,
    ERROR;

    public String toString(){
        if(this.equals(NORMAL))
            return "NORMAL msg";
        if(this.equals(WARNING))
            return "WARNING msg";
        if(this.equals(ERROR))
            return "ERROR msg";
        return null;
    }

    public boolean isHigherOrEqualLevel(LogLevel otherLevel){
        if(this.equals(ERROR))
            return true;
        if(this.equals(WARNING))
            return !otherLevel.equals(ERROR);
        if(this.equals(NORMAL))
            return otherLevel.equals(NORMAL);

        throw new IllegalArgumentException("Unimplemented code");
    }
}
