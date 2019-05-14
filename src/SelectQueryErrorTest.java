import java.io.UnsupportedEncodingException;

public class SelectQueryErrorTest {
    public static boolean SelectTableExistenceError(String tableName) throws UnsupportedEncodingException {
        return SimpleDBMSParser.findTableNumber(tableName) == -1;
    }
}
