package ae.emaratech.samples.advancedfailover;

import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Component
public class AdvancedCallback {
    public <T> CompletableFuture<T> executor(String preferredKey, long timeoutInMillSec, Map<String, CompletableFuture<Map.Entry<String, T>>> callbackMap) {
        Objects.requireNonNull(preferredKey);
        Objects.requireNonNull(callbackMap);
        if (!callbackMap.containsKey(preferredKey))
            throw new IllegalArgumentException("key is not found in callback map.");
        if (timeoutInMillSec <= 0)
            throw new IllegalArgumentException("timeoutInMillSec should be positive value greater than zero.");
        var processorBag = Sinks.one();
        // Get the current time
        Instant startTime = Instant.now();
        var publisher = callbackMap
                .values()
                .stream()
                .map(entrySupplier -> Mono.fromFuture(entrySupplier).subscribeOn(Schedulers.boundedElastic()))
                .collect(Collectors.toList());

        // Add throttling
        publisher.add(Mono.delay(Duration.ofMillis(timeoutInMillSec))
                .then(processorBag.asMono().map(m -> (Map.Entry<String, T>) m)));
        // Create an empty MonoProcessor
        var processor = Sinks.one();
        Flux.merge(publisher)
                .subscribe(
                        entry -> {
                            // Get the current time again
                            Instant endTime = Instant.now();
                            // Calculate the time elapsed in milliseconds
                            long elapsedMillis = Duration.between(startTime, endTime).toMillis();
                            System.out.println("Entry: " + entry);
                            processorBag.emitValue(entry, Sinks.EmitFailureHandler.FAIL_FAST);
                            if (entry.getKey().equalsIgnoreCase(preferredKey) || (elapsedMillis > timeoutInMillSec)) {
                                System.out.println("Result: " + entry + " elapsedMillis: " + elapsedMillis);
                                processor.emitValue(entry.getValue(), Sinks.EmitFailureHandler.FAIL_FAST);
                            }
                        },
                        error -> {
                            System.out.println("Error: " + error);
                            processor.tryEmitError(new Exception(error));
                        },
                        () -> {
                            System.out.println("Stream completed.");
                            processor.tryEmitEmpty();
                        });
        return (CompletableFuture<T>) processor.asMono().toFuture();
    }
}
