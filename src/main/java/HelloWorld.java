import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

import java.util.Collections;

public class HelloWorld {
    public static void main(String[] args) {
        IgniteConfiguration cfg=new IgniteConfiguration();
        //以客户端模式启动
        cfg.setClientMode(true);
        // 这个设置会将集群需要的类通过该应用传递到集群
        cfg.setPeerClassLoadingEnabled(true);
        TcpDiscoveryMulticastIpFinder ipFinder=new TcpDiscoveryMulticastIpFinder();
        //设置能正确匹配到服务端的ip及端口范围
        ipFinder.setAddresses(Collections.singletonList("hadoop102:47500..47509,hadoop103:47500..47509"));
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));

        //启动节点
        Ignite ignite= Ignition.start(cfg);
        //创建一个 IgniteCache 然后放进数据
        IgniteCache<Integer,String> cache=ignite.getOrCreateCache("myCache");
        cache.put(1,"Hello");
        cache.put(2,"World!");
        System.out.printf(">> Created the cache and add the values.");

        //在服务节点上执行自定义的Java计算任务
        ignite.compute(ignite.cluster().forServers()).broadcast(new RemoteTask());
        System.out.printf(">> Compute task is executed, check for output on the server nodes.");

        //客户端断开与集群的连接
        ignite.close();
    }

    /**
     * A compute tasks that prints out a node ID and some details about its OS and JRE.
     * Plus, the code shows how to access data stored in a cache from the compute task.
     */
    private static class RemoteTask implements IgniteRunnable{
        //资源注入
        @IgniteInstanceResource
        Ignite ignite;

        @Override
        public void run() {
            System.out.printf(">> Executing the compute task");

            System.out.printf(
                    "  Node ID: "+ignite.cluster().localNode().id()+"\n"+
                            "  OS: "+System.getProperty("os.name")+
                            "  JRE: "+System.getProperty("java.runtime.name")
            );

            IgniteCache<Integer,String> cache=ignite.cache("myCache");
            System.out.println(">> "+cache.get(1)+ " "+cache.get(2));
        }
    }
}
