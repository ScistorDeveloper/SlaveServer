import com.scistor.process.thrift.service.HandleNewTask;
import com.scistor.utils.ExceptionHandler;
import scala.concurrent.impl.ExecutionContextImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class threadPoolTest {
    public static void main(String[] args) {

        List<Map<String, String>> elements = new ArrayList<Map<String, String>>();

        elements.add(new HashMap<String, String>(){
            {
                put("mainclass","com.scistor.operator.DataParserOperator");
                put("filedir","");
                put("taskId","dd70842d-a946-47b7-afeb-8ded58a73432");
            }
        });
        elements.add(new HashMap<String, String>(){
            {
                put("mainclass","com.scistor.operator.GeneratePasswordBook");
                put("Host","com.scistor.ETL.operator.HostFind");
                put("task_type","producer");
                put("taskId","dd70842d-a946-47b7-afeb-8ded58a73432");
            }
        });
        elements.add(new HashMap<String, String>(){
            {
                put("mainclass","com.scistor.operator.GeneratePasswordBook");
                put("Host","com.scistor.ETL.operator.HostFind");
                put("task_type","consumer");
                put("taskId","dd70842d-a946-47b7-afeb-8ded58a73432");
            }
        });
        ExecutorService executorService = Executors.newFixedThreadPool(8);
        List<Future> futureList = new ArrayList<Future>();

        for(int i=0;i<elements.size();i++) {
            futureList.add(executorService.submit(new Thread(new Runnable() {
                @Override
                public void run() {
                    System.out.println(this.getClass().getName() + " " + Thread.currentThread().getName());
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    int x = Integer.parseInt("T");
                }
            })));
        }
        int count = 0;
        boolean runing = true;
        while (runing) {
            for (int i = 0; i < futureList.size(); i++) {
                try {
                    if (futureList.get(i).isDone()) {
                        futureList.get(i).get();
                    } else {
                        System.out.println(futureList.get(i).isCancelled() + " '" + count++);
                        futureList.get(i).get();
                        Thread.sleep(1000);
                    }
                } catch (InterruptedException e) {
                    System.out.println(String.format("handle exception in child thread. %s", e.getStackTrace()));
                    runing=false;
                } catch (ExecutionException e) {
                    System.out.println(String.format("handle exception in child thread. %s", e.getStackTrace()));
                    runing=false;
                } finally {
                    if (executorService != null) {
                        executorService.shutdown();
                    }
                }
            }
        }

//        ExecutorService threadPoolExecutor = new Executors.newFixedThreadPool(11){
//            protected void afterExecute(Runnable r, Throwable t) {
//            super.afterExecute(r, t);
//            printException(r, t);
//            }
//        };
    }

    private static void printException(Runnable r, Throwable t) {
        if (t == null && r instanceof Future<?>) {
            try {
                Future<?> future = (Future<?>) r;
                if (future.isDone())
                    future.get();
            } catch (CancellationException ce) {
                t = ce;
            } catch (ExecutionException ee) {
                t = ee.getCause();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt(); // ignore/reset
            }
        }
    }
}