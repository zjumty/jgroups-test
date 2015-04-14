package org.devzen.domain;

import com.alibaba.fastjson.JSON;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

/**
 * TODO: 这里要写注释的!
 */
@Component
public class ClusterServerImpl implements Receiver {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterServerImpl.class);

    public final String clusterName;

    @Autowired
    private Environment env;

    public ClusterServerImpl() {
        this.clusterName = "test";
    }

    private JChannel channel;

    private volatile boolean running = true;


    @PostConstruct
    public void init() throws Exception {
        String profile = env.getActiveProfiles()[0];
        String config = "msg-udp-" + profile + ".xml";
        LOG.info("initialize JChannel with config {}", config);
        InputStream cfgStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(config);

        channel = new JChannel(cfgStream);
        channel.connect(clusterName);
        channel.setDiscardOwnMessages(true);
        channel.setReceiver(this);

        new Thread(){
            @Override
            public void run() {
                while(running){
                    try {
                        broadcastMessage();
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();
    }

    @PreDestroy
    public void shutdown() {
        running = false;
        channel.disconnect();
    }

    public void broadcastMessage() throws Exception {
        Message message = new Message(null, ("hello world:" + System.currentTimeMillis() + " from " + channel.getAddressAsString()).getBytes("utf-8"));
        channel.send(message);
    }

    /**
     * 每当有实例加入或者离开集群的时候，该方法被调用
     *
     * @param newView
     */
    @Override
    public void viewAccepted(View newView) {
//        StringBuilder sb = new StringBuilder();
//        for (Address address : newView) {
//            sb.append(address);
//            sb.append("|");
//        }
        LOG.info("viewAccepted:{}", newView.toString());
    }

    @Override
    public void suspect(Address suspected_mbr) {

    }

    @Override
    public void block() {

    }

    @Override
    public void unblock() {

    }

    @Override
    public void receive(Message msg) {
        try {
            LOG.info("receive msg : {}", new String(msg.getBuffer(), "utf-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void getState(OutputStream output) throws Exception {

    }

    @Override
    public void setState(InputStream input) throws Exception {

    }
}