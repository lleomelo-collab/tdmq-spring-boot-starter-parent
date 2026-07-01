package com.eqxiu.tdmq;

import com.eqxiu.tdmq.example.TestBusEvent;
import com.seelyn.tdmq.producer.EventBusPublisher;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TdmqSpringBootStarterApplicationTests {

    @SuppressWarnings("unchecked")
    @Test
    void contextLoads() throws PulsarClientException {

        PulsarClient pulsarClient = mock(PulsarClient.class);
        ProducerBuilder<TestBusEvent> producerBuilder = mock(ProducerBuilder.class);
        Producer<TestBusEvent> producer = mock(Producer.class);

        when(pulsarClient.newProducer(any(Schema.class))).thenReturn(producerBuilder);
        when(producerBuilder.messageRoutingMode(any(MessageRoutingMode.class))).thenReturn(producerBuilder);
        when(producerBuilder.hashingScheme(any(HashingScheme.class))).thenReturn(producerBuilder);
        when(producerBuilder.topic(anyString())).thenReturn(producerBuilder);
        when(producerBuilder.create()).thenReturn(producer);

        EventBusPublisher<TestBusEvent> eventBusPublisher = new EventBusPublisher<>(pulsarClient);

        TestBusEvent event = new TestBusEvent();
        event.setSource("test21");

        eventBusPublisher.publishEvent("persistent://pulsar-m93253wq27/eqx-scs-test/demo2", event);

        verify(producer).send(event);
    }
}
