package rui.tools.rabbitmq;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class ApiOperationTest {
    private ApiOperation apiOperation = ApiOperation.builder()
            .hostName("10.1.1.234")
            .username("zhaorui")
            .password("123456")
            .build();

    @Nested
    class exchange {
        @Test
        void exchangeNames() throws IOException {
            apiOperation.exchangeNames()
                    .forEach(System.out::println);
        }

        @Test
        void exchangeNamesByVhost() throws IOException {
            apiOperation.exchangeNamesByVhost()
                    .forEach(System.out::println);
        }

        @Test
        void deleteExchange() throws IOException {
            apiOperation.deleteExchange("YZ_coupon");

        }

        @Test
        void deleteCouldDelete() throws IOException {
            apiOperation.exchangeNames()
                    .forEach(s -> {
                        if (s.contains("amq")) {
                            System.out.println("filter--->" + s);
                        } else {
                            try {
                                apiOperation.deleteExchange(s);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    });
        }
    }

    @Nested
    class Queue {
        @Test
        void queueNames() throws IOException {
            apiOperation.queueNames()
                    .forEach(System.out::println);
        }

        @Test
        void queueNamesByVhost() throws IOException {
            apiOperation.queueNamesByVhost()
                    .forEach(System.out::println);
        }

        @Test
        void purgeQueue() throws IOException {
            apiOperation.purgeQueue("a_ztx");
        }

        @Test
        void deleteQueue() throws IOException {
            apiOperation.deleteQueue("a_ztx");
        }

        @Test
        void deleteAllQueues() throws IOException {
            apiOperation.queueNames()
                    .forEach(s -> {
                        try {
                            apiOperation.deleteQueue(s);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
        }
    }

}