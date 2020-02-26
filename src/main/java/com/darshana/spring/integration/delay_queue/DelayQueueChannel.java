/*
 * Copyright 2002-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.darshana.spring.integration.delay_queue;

import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.store.MessageGroupQueue;
import org.springframework.integration.util.UpperBound;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Simple implementation of a Delayed Queue Message Channel.
 * Each {@link Message} is placed in a {@link DelayQueue}
 * whose capacity may be specified upon construction.
 * The capacity must be a positive integer value.
 *
 * @author Tharindu Darshana
 * @version 1.0.0.0
 */
public class DelayQueueChannel extends QueueChannel {

    private final UpperBound upperBound;
    private long delay;
    private TimeUnit timeUnit;

    /**
     * Create a channel with an unbounded queue. Message priority will be
     * based on the value of {@link IntegrationMessageHeaderAccessor#getPriority()}.
     */
    public DelayQueueChannel(long delay, TimeUnit timeUni) {
        this(delay, timeUni, 0);
    }

    /**
     * Create a channel with the specified queue capacity. Message priority
     * will be based upon the value of {@link IntegrationMessageHeaderAccessor#getPriority()}.
     *
     * @param capacity The queue capacity.
     */
    public DelayQueueChannel(int capacity) {
        this(capacity, null);
    }

    /**
     * Creates a channel with specific delay, and queue capacity. If the capacity
     * is a non-positive value, the queue will be unbounded. QueueChannel expects
     * a queue capable of holding any Message, but DelayQueue requires its elements
     * to implement Delayed interface. So we MUST override doSend so we control
     * what is inserted into the queue.
     *
     * @param delay    The delay
     * @param timeUnit The time units {@link TimeUnit} of the delay
     * @param capacity The capacity
     */
    public DelayQueueChannel(long delay, TimeUnit timeUnit, int capacity) {
        super((BlockingQueue) new DelayQueue());
        this.delay = delay;
        this.timeUnit = timeUnit;
        this.upperBound = new UpperBound(capacity);
    }

    /**
     * Create a channel based on the provided {@link MessageGroupQueue}.
     *
     * @param messageGroupQueue the {@link MessageGroupQueue} to use.
     * @since 5.0
     */
    public DelayQueueChannel(MessageGroupQueue messageGroupQueue) {
        super(messageGroupQueue);
        this.upperBound = new UpperBound(0);
    }

    @Override
    public int getRemainingCapacity() {
        return this.upperBound.availablePermits();
    }

    @Override
    protected boolean doSend(Message<?> message, long timeout) {
        if (!this.upperBound.tryAcquire(timeout)) {
            return false;
        } else {
            return super.doSend(new MessageWrapper(message), timeout);
        }
    }

    @Override
    protected Message<?> doReceive(long timeout) {
        Message<?> message = super.doReceive(timeout);
        if (message != null) {
            message = ((MessageWrapper) message).getRootMessage();
            this.upperBound.release();
        }
        return message;
    }

    /**
     * QueueChannel requires its elements to implement Message, and
     * DelayQueue requires its elements to implement Delayed. This
     * class implements both to satisfy these requirements. For Mesage
     * interface it acts as a proxy and forwards all calls to the wrapped Message.
     */
    protected final class MessageWrapper implements Delayed, Message<Object> {

        private final long createdClock;
        private final Message<?> rootMessage;

        MessageWrapper(Message<?> wrappedMessage) {
            this.rootMessage = wrappedMessage;
            createdClock = System.currentTimeMillis();
        }

        public long getDelay(TimeUnit unit) {
            long millisPassed = System.currentTimeMillis() - createdClock;
            long unitsPassed = unit.convert(millisPassed, TimeUnit.MILLISECONDS);
            long delayInGivenUnits = unit.convert(delay, timeUnit);
            return delayInGivenUnits - unitsPassed;
        }

        public int compareTo(Delayed o) {
            if (o instanceof MessageWrapper)
                return Long.compare(createdClock, ((MessageWrapper) o).createdClock);
            return Long.compare(getDelay(TimeUnit.NANOSECONDS), o.getDelay(TimeUnit.NANOSECONDS));
        }

        public Message<?> getRootMessage() {
            return this.rootMessage;
        }

        @Override
        public MessageHeaders getHeaders() {
            return rootMessage.getHeaders();
        }

        @Override
        public Object getPayload() {
            return rootMessage.getPayload();
        }

    }

}
