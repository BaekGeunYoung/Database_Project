public class CustomException {
    static class TempException extends Exception{
        private String message = "";
        public TempException(String message){
            this.message = message;
        }
        public String toString() {
            return message;
        }
    }
}
