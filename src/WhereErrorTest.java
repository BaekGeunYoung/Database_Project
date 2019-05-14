public class WhereErrorTest {
    public static boolean WhereIncomparableError(String op1, String op2){
        String regex_int = "([\\-])?([0-9])+";
        String regex_char = "['][\\s\\S]*[']";
        String regex_date = "[0-9]{4}[\\-][0-9]{2}[\\-][0-9]{2}";

        if(op1.equals("null") || op2.equals("null")) return false;
        if(op1.matches(regex_int) && op2.matches(regex_int)) return false;
        if(op1.matches(regex_char) && op2.matches(regex_char)) return false;
        if(op1.matches(regex_date) && op2.matches(regex_date)) return false;
//
//        System.out.println("Where clause try to compare imcomparable values");
        return true;
    }
}
