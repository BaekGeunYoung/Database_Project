import com.sleepycat.je.*;
import java.io.UnsupportedEncodingException;
import java.util.*;

public class InsertQueryErrorTest {
    public static boolean NoSuchTableError(Database db, String tableName) throws UnsupportedEncodingException {
        if (SimpleDBMSParser.findTableNumber(tableName) == -1){
            System.out.println("DB_2017-17450> No such table");
            return true;
        }

        return false;
    }
    public static boolean InsertTypeMismatchError(Database db, String valueList, int tableNum) throws UnsupportedEncodingException {
        String[] types = SimpleDBMSParser.getData("type" + tableNum).split(",", -1);
        String[] values = valueList.split(",", -1);
        String regex_int = "([\\-])?([0-9])+";
        String regex_char = "['][\\s\\S]*[']";
        String regex_date = "[0-9]{4}[\\-][0-9]{2}[\\-][0-9]{2}";

        if(values.length != types.length) {
            System.out.println("DB_2017-17450> Insertion has failed: Types are not matched");
            return true; /*개수 검사*/
        }

        for(int i = 0 ; i < values.length ; i++){
            if(values[i].equals("null")) break;

            if(values[i].matches(regex_int)) {
                if (!types[i].equals("int")) {
                    System.out.println("DB_2017-17450> Insertion has failed: Types are not matched");
                    return true;
                }
            }
            else if(values[i].matches(regex_char)) {
                if (!types[i].startsWith("char")) {
                    System.out.println("DB_2017-17450> Insertion has failed: Types are not matched");
                    return true;
                }


            }
            else if(values[i].matches(regex_date)) {
                if (!types[i].equals("date")) {
                    System.out.println("DB_2017-17450> Insertion has failed: Types are not matched");
                    return true;
                }
            }
            else {
                System.out.println("DB_2017-17450> Insertion has failed: Types are not matched");
                return true;
            }
        }

        return false;
    }
    public static boolean InsertColumnNonNullableError(Database db, String valueList, int tableNum) throws ParseException, UnsupportedEncodingException {
        SimpleDBMSParser.arrayUpdate(tableNum);
        String[] isnull = SimpleDBMSParser.isnull;
        String[] values = valueList.split(",", -1);

        for(int i = 0 ; i < values.length ; i++){
            if(values[i].equals("null")){
                if(isnull[i].equals("N")){
                    System.out.println("DB_2017-17450> Insertion has failed: " + SimpleDBMSParser.column_name[i] + " is not nullable");
                    return true;
                }
            }
        }

        return false;
    }
    public static boolean InsertColumnExistenceError(Database db, String columnList, String tableName) throws UnsupportedEncodingException {
        String[] columns = columnList.split(",",-1);
        String[] allCol = SimpleDBMSParser.findColData(tableName);
        int r = 0;
        for(int i = 0 ; i < columns.length ; i++){
            r = 0;
            for(int j = 0 ; j < allCol.length ; j++){
                if(columns[i].equals(allCol[j])){
                    r = 1;
                    break;
                }
            }
            if(r == 0){
                System.out.println("DB_2017-17450> Insertion has failed: " + columns[i] + " does not exist");
                return true;
            }
        }

        return false;
    }
    public static boolean InsertDuplicatePrimaryKeyError(Database db, String valueList, int tableNum) throws ParseException, UnsupportedEncodingException {
        SimpleDBMSParser.arrayUpdate(tableNum);

        String[] keyInfo = SimpleDBMSParser.key_info;
        String[] values = valueList.split(",", -1);
        String[] tuples = SimpleDBMSParser.allTuple(SimpleDBMSParser.getData("table" + tableNum));
        ArrayList<Integer> pkIndex = new ArrayList<>();

        for(int i = 0 ; i < values.length ; i++){
            if(keyInfo[i].equals("PRI")){
                pkIndex.add(i);
            }
        }

        if(pkIndex.size() == 0) return false;

        int r = 0;

        for(int i = 0 ; i < tuples.length ; i++){
            r = 0;
            String[] tupleElement = tuples[i].split(",", -1);
            for(int j = 0 ; j < pkIndex.size() ; j++){
                if(!tupleElement[pkIndex.get(j)].equals(values[pkIndex.get(j)])){
                    r = 1;
                    break;
                }
            }
            if(r == 0){
                System.out.println("DB_2017-17450> Insertion has failed: Primary key duplication");
                return true;
            }
        }

        return false;
    }
    public static boolean InsertReferentialIntegrityError(Database db, String valueList, int tableNum) throws ParseException, UnsupportedEncodingException {
        String[] values = valueList.split(",", -1);

        if(SimpleDBMSParser.getData("refTable" + tableNum).equals("")) return false;

        String[] refTableList = SimpleDBMSParser.getData("refTable" + tableNum).split(",", -1);
        String[] refEachList = SimpleDBMSParser.getData("refList" + tableNum).split(" ", -1);
        String[] fkEachList = SimpleDBMSParser.getData("foreign" + tableNum).split(" ", -1);
//
//        System.out.println("reftable: " + refTableList[0]);
//        System.out.println("reflist: " + refEachList[0]);
//        System.out.println("foreign: " + fkEachList[0]);
        int r = 0;

        for(int j = 0 ; j < refTableList.length ; j++){
            String tableName = refTableList[j];

            String[] tuples = SimpleDBMSParser.allTuple(tableName);
            ArrayList<Integer> fkIndex = new ArrayList<>();
            ArrayList<Integer> refIndex = new ArrayList<>();

            String[] fkAttrList = fkEachList[j].split(",", -1);
            String[] refAttrList = refEachList[j].split(",", -1);
            String[] fkColList = SimpleDBMSParser.findColData(SimpleDBMSParser.getData("table" + tableNum));
            String[] refColList = SimpleDBMSParser.findColData(tableName);

            for(int i = 0 ; i < fkAttrList.length ; i++){
                fkIndex.add(Arrays.asList(fkColList).indexOf(fkAttrList[i]));
                refIndex.add(Arrays.asList(refColList).indexOf(refAttrList[i]));
            }

            int c = 0;
            for(int i = 0 ; i < tuples.length ; i++){
                c = 0;
                String[] tupleElement = tuples[i].split(",", -1);
                for(int k = 0 ; k < fkIndex.size() ; k++){
                    if(!tupleElement[refIndex.get(k)].equals(values[fkIndex.get(k)])){
                        c = 1;
                        break;
                    }
                }
                if(c == 0){
                    r++;
                    break;
                }
            }
        }

        if(r == refTableList.length) return false;

        System.out.println("DB_2017-17450> Insertion has failed: Referential integrity violation");
        return true;
    }
    public static String InsertTruncateCharString(Database db, String valueList, int tableNum) throws UnsupportedEncodingException {
        String[] types = SimpleDBMSParser.getData("type" + tableNum).split(",", -1);
        String[] values = valueList.split(",", -1);
        String regex_char = "['][\\s\\S]*[']";

        for(int i = 0 ; i < values.length ; i++){
            if(values[i].equals("null")) break;

            else if(values[i].matches(regex_char)) {
                int num = Integer.parseInt(types[i].substring(5, types[i].length() - 1));
                if(values[i].length()-2 > num)values[i] = values[i].substring(0, num + 1) + "'";
            }
        }

        String result = values[0];
        for(int i = 1 ; i < values.length ; i++) result += "," + values[i];

        return result;
    }
    public static void InsertQuerySuccess(){
        System.out.println("DB_2017-17450> The row is inserted");
    }
}
