import com.scistor.process.thrift.service.HandleNewTask;
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
                put("filedir","sdf");
            }
        });
        elements.add(new HashMap<String, String>(){
            {
                put("mainclass","com.scistor.ETL.operator.HostFind");
                put("Host","com.scistor.ETL.operator.HostFind");
                put("task_type","producer");
            }
        });
        elements.add(new HashMap<String, String>(){
            {
                put("mainclass","com.scistor.ETL.operator.HostFind");
                put("Host","com.scistor.ETL.operator.HostFind");
                put("task_type","producer");
            }
        });
        elements.add(new HashMap<String, String>(){
            {
                put("mainclass","com.scistor.ETL.operator.HostFind");
                put("Host","com.scistor.ETL.operator.HostFind");
                put("task_type","producer");
            }
        });
        elements.add(new HashMap<String, String>(){
            {
                put("mainclass","com.scistor.ETL.operator.HostFind");
                put("Host","com.scistor.ETL.operator.HostFind");
                put("task_type","consumer");
            }
        });
        ExecutorService executorService = Executors.newFixedThreadPool(8);
        List<Future> futureList = new ArrayList<Future>();

        for(int i=0;i<elements.size();i++) {
            futureList.add(executorService.submit(new HandleNewTask(elements.get(i), null, null)));
        }
        for(int i=0;i<8;i++) {
            try {
                futureList.get(i).get();
            } catch (InterruptedException e) {
                System.out.println(String.format("handle exception in child thread. %s", e.getStackTrace()));
            } catch (ExecutionException e) {
                System.out.println(String.format("handle exception in child thread. %s", e.getStackTrace()));
            } finally {
                if (executorService != null) {
                    executorService.shutdown();
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