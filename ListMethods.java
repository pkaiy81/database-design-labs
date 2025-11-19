import java.sql.DatabaseMetaData;
import java.lang.reflect.Method;

public class ListMethods {
    public static void main(String[] args) {
        for (Method m : DatabaseMetaData.class.getDeclaredMethods()) {
            System.out.println(m.getName());
        }
    }
}
