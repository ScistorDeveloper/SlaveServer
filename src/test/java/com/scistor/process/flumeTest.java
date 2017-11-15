package com.scistor.process;
import com.scistor.operator.FlumeClientOperator;

public class flumeTest {

    public static void main(String[] args) {
        try {
            FlumeClientOperator.sendDataToFlume("this is flume test send a string");
            FlumeClientOperator.cleanUp();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
