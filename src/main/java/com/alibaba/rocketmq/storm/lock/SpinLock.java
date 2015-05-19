package com.alibaba.rocketmq.storm.lock;

import java.util.concurrent.atomic.AtomicReference;

public class SpinLock {

    private AtomicReference<Thread> owner = new AtomicReference<>();

    private int count;


    public void lock() {
        Thread current = Thread.currentThread();
        if (current == owner.get()) {
            count++;
            return;
        }

        while (!owner.compareAndSet(null, current)) {
            // CAS operation.
        }
    }


    public void unlock() {
        Thread current = Thread.currentThread();
        if (current == owner.get()) {
            if (count > 0) {
                count--;
            } else {
                while (!owner.compareAndSet(current, null)) {
                    // CAS operation.
                }
            }
        }
    }
}
