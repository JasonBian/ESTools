package com.zexin.tools;


import org.softee.management.annotation.Description;
import org.softee.management.annotation.MBean;
import org.softee.management.annotation.ManagedAttribute;
import org.softee.management.exception.ManagementException;
import org.softee.management.helper.MBeanRegistration;

import javax.management.ObjectName;
import java.util.concurrent.*;

/**
 * Created by bianzexin on 16/12/19.
 */
@MBean
@Description("MBeanThreadPoolExecutor")
public class MBeanThreadPoolExecutor extends ThreadPoolExecutor {
    MBeanRegistration registration;

    public MBeanThreadPoolExecutor(String objectName, int corePoolSize) {
        this(objectName, corePoolSize, corePoolSize, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
                Executors.defaultThreadFactory(), new ThreadPoolExecutor.DiscardPolicy());
    }

    public MBeanThreadPoolExecutor(String objectName, int corePoolSize, int maximumPoolSize, long keepAliveTime,
                                   TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory,
                                   RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
        try {
            registration = new MBeanRegistration(this, new ObjectName(objectName));
            registration.register();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void shutdown() {
        super.shutdown();
        try {
            if (registration != null) {
                registration.unregister();
            }
        } catch (ManagementException e) {
            e.printStackTrace();
        }
    }

    @ManagedAttribute
    @Description("Returns the approximate number of threads that are actively executing tasks.")
    public int getActiveCount4Jmx() {
        return this.getActiveCount();
    }

    @ManagedAttribute
    @Description("Returns the approximate total number of tasks that have completed execution. Because the states of tasks and threads may change dynamically during computation, the returned value is only an approximation, but one that does not ever decrease across successive calls.")
    public long getCompletedTaskCount4Jmx() {
        return this.getCompletedTaskCount();
    }

    @ManagedAttribute
    @Description("Returns the largest number of threads that have ever simultaneously been in the pool.")
    public int getLargestPoolSize4Jmx() {
        return this.getLargestPoolSize();
    }

    @ManagedAttribute
    @Description("Returns the current number of threads in the pool.")
    public int getPoolSize4Jmx() {
        return this.getPoolSize();
    }

    @ManagedAttribute
    @Description("Returns the approximate total number of tasks that have ever been scheduled for execution. Because the states of tasks and threads may change dynamically during computation, the returned value is only an approximation.")
    public long getTaskCount4Jmx() {
        return this.getTaskCount();
    }

    @ManagedAttribute
    @Description("Returns the current number of tasks that have ever simultaneously been in the queue.")
    public int getQueueSize4Jmx() {
        return this.getQueue().size();
    }
}
