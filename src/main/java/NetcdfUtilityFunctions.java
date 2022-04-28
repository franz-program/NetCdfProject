import ucar.nc2.Variable;

public class NetcdfUtilityFunctions {


    public static boolean isDependent(Variable variable) {
        return variable.getDimensions().size() > 1;
    }

    public static float getFillValue(Variable v){
        //TODO: da fare
        return 0;
    }

}
