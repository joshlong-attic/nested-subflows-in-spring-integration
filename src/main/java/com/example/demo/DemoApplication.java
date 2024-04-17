package com.example.demo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.dsl.DirectChannelSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.splitter.AbstractMessageSplitter;
import org.springframework.integration.transformer.AbstractTransformer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;

import java.lang.annotation.*;
import java.util.HashSet;
import java.util.Set;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }


    @Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.TYPE, ElementType.ANNOTATION_TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    @Inherited
    @Documented
    @Qualifier(ORDER_FLOW)
    @interface Requests {
    }

    private static final String REQUESTS_MESSAGE_CHANNEL = "requests";
    private static final String ORDER_FLOW = "orderIntegrationFlow";

    @Bean
    ApplicationRunner demo(@Requests MessageChannel messageChannel) {
        return args -> {


            var joshOrder = new Order(1, Set.of(new LineItem(11, "11"), new LineItem(22, "12")));
            var rodOrder = new Order(2, Set.of(new LineItem(33, "33"), new LineItem(44, "44")));

            messageChannel.send(MessageBuilder.withPayload(Set.of (joshOrder ,rodOrder)).build());

//            messageChannel.send(MessageBuilder.withPayload(rodOrder).build());

        };
    }


    // todo look into IntegrationFlowContext
    //  for dynamic launching of parameterized flows
    //  https://docs.spring.io/spring-integration/api/org/springframework/integration/dsl/context/IntegrationFlowContext.html

    @Requests
    @Bean
    DirectChannelSpec requestsMessageChannel() {
        return MessageChannels.direct();
    }

    @Bean
    IntegrationFlow mainFlow(
            @Qualifier(ORDER_FLOW) IntegrationFlow subflowPerOrder,
            @Requests MessageChannel requestsMessageChannel) {
        return IntegrationFlow
                .from(requestsMessageChannel)
                .split()
                .gateway(subflowPerOrder)
                .aggregate()
                .handle(message -> {
                    System.out.println("end of the line! " + message.getPayload() + " " + message.getHeaders());
                })
                .get();
    }


    @Bean(ORDER_FLOW)
    IntegrationFlow subflowPerOrder() {
        return flow -> flow
                .split(new AbstractMessageSplitter() {

                    @Override
                    protected Object splitMessage(Message<?> message) {
                        var order = (Order) message.getPayload();
                        var messages = new HashSet<Message<LineItem>>();
                        for (var o : order.lineItems())
                            messages.add(MessageBuilder.withPayload(o).setHeader("order", order).build());
                        return messages;
                    }
                })
                .transform(new AbstractTransformer() {
                    @Override
                    protected Object doTransform(Message<?> message) {
                        System.out.println("original order: " + message.getHeaders().get("order"));
                        var li = (LineItem) message.getPayload();
                        return MessageBuilder
                                .withPayload(li)
                                .copyHeadersIfAbsent(message.getHeaders())
                                .build();
                    }
                })
                .aggregate()

                .log(m -> "got the aggregated values " + m.getPayload())
                ;


    }
}


record LineItem(Integer id, String sku) {
}

record Order(Integer id, Set<LineItem> lineItems) {
}