public class ColumnInfo {

    //TODO: fai il get info totale

    private String name;
    private float fillValue;

    public ColumnInfo(String name, float fillValue){
        this.name = name;
        this.fillValue = fillValue;
    }

    public String getName(){
        return name;
    }

    public float getFillValue() {
        return fillValue;
    }

    public static String[] getKindOfInfo(){
        return new String[]{"name", "fillValue"};
    }

    @Override
    public boolean equals(Object obj) {

        if(!(obj instanceof ColumnInfo))
            return false;

        //TODO: errori se ci sono fill value diversi
        //TODO: da rifare con il getInfo
        ColumnInfo other = (ColumnInfo) obj;
        return this.name.equals(other.getName());
    }
}
