package com.darshana.spring.integration.delay_queue;


import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SpringBootTest
@EnableIntegration
public class DelayQueueChannelTest {

    private final Logger logger = LoggerFactory.getLogger(DelayQueueChannelTest.class);

    @Test
    public void testSimpleSendAndReceive() throws Exception {
        final AtomicBoolean messageReceived = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        final DelayQueueChannel channel = new DelayQueueChannel(100, TimeUnit.MILLISECONDS);
        new Thread(() -> {
            Message<?> message = channel.receive();
            messageReceived.set(true);
            latch.countDown();
            assertTrue(message instanceof DelayQueueChannel.MessageWrapper);
            logger.info(message.toString());
            //float waitTime = (System.currentTimeMillis() - message.getHeaders()) / 1000.F;
            //logger.info("waited for {} seconds", waitTime);
        }).start();
        assertFalse(messageReceived.get());
        channel.send(new GenericMessage(System.currentTimeMillis()));
        assertFalse(messageReceived.get());
        latch.await(25, TimeUnit.MILLISECONDS);
        assertFalse(messageReceived.get());
        latch.await(1, TimeUnit.SECONDS);
        assertTrue(messageReceived.get());
    }

}
