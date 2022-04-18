package consumer;

public class MultiThreadConsumer implements Runnable{
    private String threadName;
    private Thread t;

    MultiThreadConsumer(String name){
        threadName = name;
        System.out.println("Create " + threadName + " thread");
    }

    public void run(){
        System.out.println("Running " + threadName + "thread");
        try {
         for(int i = 4; i > 0; i--) {
            System.out.println("Thread: " + threadName + ", " + i);
            // Let the thread sleep for a while.
            Thread.sleep(2000);
         }
      } catch (InterruptedException e) {
         System.out.println("Thread " +  threadName + " interrupted.");
      }
      System.out.println("Thread " +  threadName + " exiting.");
    }

    /*
    start
         */
    public void start(){
        System.out.println("Starting " +  threadName );
      if (t == null) {
         t = new Thread (this, threadName);
         t.start ();
      }
    }
}
