package rui.tools.rabbitmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@Builder
@Slf4j
public class ApiOperation {

    private String username;

    private String password;

    @Builder.Default
    private String vhost = "%2F";

    private String hostName;

    @Builder.Default
    private int port = 15672;
    @Builder.Default
    private ObjectMapper mapper = new ObjectMapper();


    private HttpHost httpHost;


    private Executor executor;

    private static final String API = "/api";
    private static final String QUEUES = "/queues";
    private static final String EXCHANGES = "/exchanges";


    private HttpHost getHttpHost() {
        if (httpHost == null) {
            httpHost = new HttpHost(hostName, port, "http");
        }
        return httpHost;
    }

    private Executor getExecutor() {
        if (executor == null) {
            executor = Executor.newInstance()
                    .auth(getHttpHost(), username, password)
                    .authPreemptive(getHttpHost());
        }
        return executor;
    }

    public List<String> exchangeNames() throws IOException {
        String resp = getExecutor().execute(Request.Get(getHttpHost() + API + EXCHANGES))
                .returnContent()
                .asString();
        return getNames(resp);
    }

    public List<String> exchangeNamesByVhost() throws IOException {
        String resp = getExecutor().execute(Request.Get(getHttpHost() + API + EXCHANGES + "/" + vhost))
                .returnContent()
                .asString();
        return getNames(resp);
    }

    public void deleteExchange(String exchangeName) throws IOException {
        getExecutor().execute(Request.Delete(getHttpHost() + API + EXCHANGES + "/" + vhost + "/" + exchangeName));
    }

    public List<String> queueNames() throws IOException {
        String resp = getExecutor().execute(Request.Get(getHttpHost() + API + QUEUES))
                .returnContent()
                .asString();

        return getNames(resp);
    }

    public List<String> queueNamesByVhost() throws IOException {
        String resp = getExecutor().execute(Request.Get(getHttpHost() + API + QUEUES + "/" + vhost))
                .returnContent()
                .asString();

        return getNames(resp);
    }

    public void purgeQueue(String queueName) throws IOException {
        getExecutor().execute(Request
                .Delete(getQueueApiName(queueName) + "/contents"));
    }

    public void deleteQueue(String queueName) throws IOException {
        getExecutor().execute(Request
                .Delete(getQueueApiName(queueName)));
        log.info("删除队列： {}", queueName);
    }


    @SuppressWarnings("unchecked")
    private List<String> getNames(String resp) throws IOException {
        return (List<String>) mapper.readValue(resp, List.class)
                .stream()
                .map(o -> {
                    Map map = (Map) o;
                    return map.get("name").toString();
                }).collect(Collectors.toList());
    }

    private String getQueueApiName(String queueName) {
        return getHttpHost() + API + QUEUES + "/" + vhost + "/" + queueName;
    }

}
