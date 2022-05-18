public class ColumnInfo {

    private String name;
    private float fillValue;

    public ColumnInfo(String name, float fillValue) {
        this.name = name;
        this.fillValue = fillValue;
    }

    public String getName() {
        return name;
    }

    public float getFillValue() {
        return fillValue;
    }

    public Object[] getFullInfo() {
        return new Object[]{name, fillValue};
    }

    public static String[] getKindOfInfo() {
        return new String[]{"name", "fillValue"};
    }

    @Override
    public boolean equals(Object obj) {

        if (!(obj instanceof ColumnInfo))
            return false;

        ColumnInfo other = (ColumnInfo) obj;
        return this.name.equals(other.getName());
    }

}
