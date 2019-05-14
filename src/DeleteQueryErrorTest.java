import com.sleepycat.je.*;

import java.io.UnsupportedEncodingException;
import java.util.*;

public class DeleteQueryErrorTest {
    public static boolean NoSuchTable(String tableName) throws UnsupportedEncodingException {
        if (SimpleDBMSParser.findTableNumber(tableName) == -1){
            System.out.println("DB_2017-17450> No such table");
            return true;
        }

        return false;
    }

    public static boolean DeleteReferentialIntegrityPassed(String tableName, String tuple) throws UnsupportedEncodingException, ParseException {
        //true면 delete 실패, false면 성공
        String pkData = SimpleDBMSParser.getData("primary" + SimpleDBMSParser.findTableNumber(tableName));
        if(pkData.equals("")) return false;
        Integer[] indexOfPk;
        Integer[] indexOfFk;

        ArrayList<Integer> tableNums = new ArrayList<>();

        Cursor cursor = SimpleDBMSParser.myDatabase.openCursor(null, null);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();

        cursor.getFirst(key, value, LockMode.DEFAULT);
        if(key.getData() == null) {
            cursor.close();
            System.out.println("deletequeryerrortest 26");
            return false;
        }
        do {
            String keyString = new String(key.getData(), "UTF-8");
            String valueString = new String(value.getData(), "UTF-8");

            if(keyString.startsWith("refTable")) {
                String[] refTableList = valueString.split(",", -1);
                for(int i = 0 ; i < refTableList.length ; i++){
                    if(refTableList[i].equals(tableName)){
                        int tableNum = Integer.parseInt(keyString.substring(8, keyString.length()));

                        String fkList = SimpleDBMSParser.getData("foreign" + tableNum).split(" ", -1)[i];
                        String refList = SimpleDBMSParser.getData("refList" + tableNum).split(" ", -1)[i];

                        indexOfPk = makeIndex(refList, tableName);
                        String currTableName = SimpleDBMSParser.getData("table" + tableNum);
                        indexOfFk = makeIndex(fkList, currTableName);

                        if(examineReferentialIntegrity(tableNum, tuple, indexOfPk, indexOfFk)){
                            cursor.close();
                            return true;
                        }
                    }
                }
            }
        } while(cursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS);
        cursor.close();

//여기로 왔다는건 integrity constraint를 위배하지 않는다는 뜻?
//아래는 nullable한 친구들 null 값으로 바꿔주기

        cursor = SimpleDBMSParser.myDatabase.openCursor(null, null);
        cursor.getFirst(key, value, LockMode.DEFAULT);
        if(key.getData() == null) {
            cursor.close();
            System.out.println("deletequeryerrortest 69");
            return false;
        }
        do {
            String keyString = new String(key.getData(), "UTF-8");
            String valueString = new String(value.getData(), "UTF-8");

            if(keyString.startsWith("refTable")) {
                String[] refTableList = valueString.split(",", -1);
                for(int i = 0 ; i < refTableList.length ; i++){
                    if(refTableList[i].equals(tableName)){
                        int tableNum = Integer.parseInt(keyString.substring(8, keyString.length()));

                        String fkList = SimpleDBMSParser.getData("foreign" + tableNum).split(" ", -1)[i];
                        String refList = SimpleDBMSParser.getData("refList" + tableNum).split(" ", -1)[i];

                        indexOfPk = makeIndex(refList, tableName);
                        String currTableName = SimpleDBMSParser.getData("table" + tableNum);
                        indexOfFk = makeIndex(fkList, currTableName);

                        changeNullableValue(tableNum, tuple, indexOfPk, indexOfFk);
                    }
                }
            }
        } while(cursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS);
        cursor.close();


        return false;
    }

    public static Integer[] makeIndex(String keyList, String tableName) throws UnsupportedEncodingException {
        ArrayList<Integer> temp = new ArrayList<>();

        String[] colList = SimpleDBMSParser.findColData(tableName);
        String[] keys = keyList.split(",", -1);

        for(int i = 0 ; i < keys.length ; i++){
            temp.add(Arrays.asList(colList).indexOf(keys[i]));
        }
        return temp.toArray(new Integer[temp.size()]);
    }

    public static void changeNullableValue(int tableNum, String tuple, Integer[] indexOfPk, Integer[] indexOfFk) throws UnsupportedEncodingException, ParseException {
        SimpleDBMSParser.arrayUpdate(tableNum);
        String currTableName = SimpleDBMSParser.getData("table" + tableNum);
        System.out.println(currTableName);
        String[] alltuple = SimpleDBMSParser.allTuple(currTableName);
        String[] tupleElement = tuple.split(",", -1);

        int r = 0;
        for(int i = 0 ; i < alltuple.length ; i++){
            String[] cmpElement = alltuple[i].split(",", -1);
            r = 0;
            for(int j = 0 ; j < indexOfFk.length ; j++){
                if(!cmpElement[indexOfFk[j]].equals(tupleElement[indexOfPk[j]])){
                    r = 1;
                    break;
                }
            }
            if(r == 0){
                int c = 0;
                for(int j = 0 ; j < indexOfFk.length ; j++){
                    if(SimpleDBMSParser.isnull[indexOfFk[j]].equals("N")){
                        c = 1;
                        break;
                    }
                }
                if(c == 0){ //전부 다 nullable value다 -> 이 값들을 null로 바꾸고 delete 가능!!
                    for(int j = 0 ; j < indexOfFk.length ; j++){
                        cmpElement[indexOfFk[j]] = "null";
                    }
                    alltuple[i] = cmpElement[0];
                    for(int j = 1 ; j < cmpElement.length ; j++) alltuple[i] += "," + cmpElement[j];
                }
                else System.out.println("FATAL ERROR");
            }
        }

        String tupleString = alltuple.length == 0 ? "" : alltuple[0];
        for(int i = 1 ; i < alltuple.length ; i++) tupleString += "," + alltuple[i];

        SimpleDBMSParser.InsertData(tableNum, tupleString);
    }

    public static boolean examineReferentialIntegrity(int tableNum, String tuple, Integer[] indexOfPk, Integer[] indexOfFk) throws UnsupportedEncodingException, ParseException {
        //true면 delete 실패, false면 성공(nullable한 value change는 아직 안함)
        SimpleDBMSParser.arrayUpdate(tableNum);
        String[] alltuple = SimpleDBMSParser.allTuple(SimpleDBMSParser.getData("table" + tableNum));
        String[] tupleElement = tuple.split(",", -1);

        int r = 0;
        for(int i = 0 ; i < alltuple.length ; i++){
            String[] cmpElement = alltuple[i].split(",", -1);
            r = 0;
            for(int j = 0 ; j < indexOfFk.length ; j++){
                if(!cmpElement[indexOfFk[j]].equals(tupleElement[indexOfPk[j]])){
                    r = 1;
                    break;
                }
            }
            if(r == 0){
                int c = 0;
                for(int j = 0 ; j < indexOfFk.length ; j++){
                    if(SimpleDBMSParser.isnull[indexOfFk[j]].equals("N")){
                        c = 1;
                        break;
                    }
                }
                if(c == 1) return true;
            }
        }

        return false;
    }
}
