package org.app;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) {
        ExecutorService executorService= Executors.newFixedThreadPool(3);
        for(int i=0;i<100;++i) {
            final int k=i;
            executorService.execute(() -> {
                System.out.println("ceferfcefce" + k);
            });
        }
        executorService.shutdown();
    }
}
