public class KeepAlive {
    /**
     * System.out.println utility method
     *
     * @param value : value to print
     */
    static void print(String value) {
        System.out.println(value);
    }
    /**
     * main method for this class
     */
    public static void main(String[] args) {
        print("Starting KeepAlive loop...");
        while(true){
          try {
              Thread.sleep(60 * 1000);
          } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
          }
        }
    }
}
