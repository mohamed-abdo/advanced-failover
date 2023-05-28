package ae.emaratech.samples.advancedfailover;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
public class Controller {
    private final ResilientParallelExecutor resilientParallelExecutor;

    public Controller(ResilientParallelExecutor resilientParallelExecutor) {
        this.resilientParallelExecutor = resilientParallelExecutor;
    }

    @GetMapping({"", "/"})
    public Mono<String> get() {
        var result = resilientParallelExecutor.execute("server-1", Duration.ofSeconds(5), Map.of(
                "server-1", CompletableFuture.supplyAsync(() -> demoServer("server-1",1000)),
                "server-2", CompletableFuture.supplyAsync(() -> demoServer("server-2",500)),
                "server-3", CompletableFuture.supplyAsync(() -> demoServer("server-3",1900))
        ));
        return Mono.fromFuture(result);
    }

    private Map.Entry<String, String> demoServer(String serverId, long respondInMilli) {
        System.out.println(serverId + " 1 start computing ....");
        try {
            Thread.sleep(respondInMilli);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println(serverId + " end computing .... in: " + respondInMilli);
        return new AbstractMap.SimpleEntry<>(serverId, " server response in: " + respondInMilli);
    }
}
