package ae.emaratech.samples.advancedfailover;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@ActiveProfiles(profiles = {"test"})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class AdvancedFailoverApplicationTests {
    @Autowired
    private ResilientParallelExecutor resilientParallelExecutor;

    @Test
    void contextLoads() {
    }

    private Map.Entry<String, String> demoServer(String serverId, long respondInMilli) {
        System.out.println(serverId + " 1 start computing ....");
        try {
            Thread.sleep(respondInMilli);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println(serverId + " end computing .... in: " + respondInMilli);
        return new AbstractMap.SimpleEntry<>(serverId, serverId);
    }

    @Test
    public void testPreferredIsTheFastest() {
        var result = resilientParallelExecutor.execute("server-1", Duration.ofSeconds(5), Map.of(
                "server-1", CompletableFuture.supplyAsync(() -> demoServer("server-1", 1000)),
                "server-2", CompletableFuture.supplyAsync(() -> demoServer("server-2", 500)),
                "server-3", CompletableFuture.supplyAsync(() -> demoServer("server-3", 1900))
        ));
        var actual = Mono.fromFuture(result).block();
        Assertions.assertEquals("server-1", actual);
    }

    @Test
    public void testNonPreferredIsTheFastest() {
        var result = resilientParallelExecutor.execute("server-1", Duration.ofSeconds(5), Map.of(
                "server-1", CompletableFuture.supplyAsync(() -> demoServer("server-1", 6000)),
                "server-2", CompletableFuture.supplyAsync(() -> demoServer("server-2", 500)),
                "server-3", CompletableFuture.supplyAsync(() -> demoServer("server-3", 1900))
        ));
        var actual = Mono.fromFuture(result).block();
        Assertions.assertEquals("server-2", actual);
    }

    @Test
    public void testPreferredIsTheFastestEvenWithTimeout() {
        var result = resilientParallelExecutor.execute("server-3", Duration.ofSeconds(5), Map.of(
                "server-1", CompletableFuture.supplyAsync(() -> demoServer("server-1", 7500)),
                "server-2", CompletableFuture.supplyAsync(() -> demoServer("server-2", 8000)),
                "server-3", CompletableFuture.supplyAsync(() -> demoServer("server-3", 6700))
        ));
        var actual = Mono.fromFuture(result).block();
        Assertions.assertEquals("server-3", actual);
    }
}
