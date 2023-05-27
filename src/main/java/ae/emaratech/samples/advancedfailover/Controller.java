package ae.emaratech.samples.advancedfailover;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
public class Controller {
    private final AdvancedCallback advancedCallback;

    public Controller(AdvancedCallback advancedCallback) {
        this.advancedCallback = advancedCallback;
    }

    @GetMapping({"", "/"})
    public Mono<String> get() {
        var result = advancedCallback.executor("server-1", 5000, Map.of(
                "server-1", CompletableFuture.supplyAsync(() -> serve1(7000)),
                "server-2", CompletableFuture.supplyAsync(() -> serve2(3500)),
                "server-3", CompletableFuture.supplyAsync(() -> serve3(700))
        ));
        return Mono.fromFuture(result);
    }

    private Map.Entry<String, String> serve1(long respondInMilli) {
        System.out.println("Serve 1 start computing ....");
        try {
            Thread.sleep(respondInMilli);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Serve 1 end computing .... in: " + respondInMilli);
        return new AbstractMap.SimpleEntry<>("server-1", "server 1 response");
    }

    private Map.Entry<String, String> serve2(long respondInMilli) {
        System.out.println("Serve 2 start computing ....");
        try {
            Thread.sleep(respondInMilli);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Serve 2 end computing .... in: " + respondInMilli);
        return new AbstractMap.SimpleEntry<>("server-2", "server 2 response");
    }
    private Map.Entry<String, String> serve3(long respondInMilli) {
        System.out.println("Serve 3 start computing ....");
        try {
            Thread.sleep(respondInMilli);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Serve 3 end computing .... in: " + respondInMilli);
        return new AbstractMap.SimpleEntry<>("server-3", "server 3 response");
    }
}
