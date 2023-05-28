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
public class ResilientParallelExecutor {
    public <T> CompletableFuture<T> execute(String preferredExecutionKey, Duration duration, Map<String, CompletableFuture<Map.Entry<String, T>>> futureExecutors) {
        Objects.requireNonNull(preferredExecutionKey);
        Objects.requireNonNull(duration);
        Objects.requireNonNull(futureExecutors);
        if (!futureExecutors.containsKey(preferredExecutionKey))
            throw new IllegalArgumentException("key is not found in callback map.");
        if (duration.getSeconds() <= 0)
            throw new IllegalArgumentException("duration should be positive value greater than zero.");
        // Get the current time
        Instant startTime = Instant.now();
        var publisher = futureExecutors
                .values()
                .stream()
                .map(entrySupplier -> Mono.fromFuture(entrySupplier)
                        .subscribeOn(Schedulers.boundedElastic()))
                .collect(Collectors.toList());

        // Add throttling
        var processorBag = Sinks.one();
        publisher.add(Mono.delay(duration)
                .then(processorBag.asMono().map(m -> (Map.Entry<String, T>) m)));
        // Create an empty MonoProcessor
        var processor = Sinks.one();
        Flux.merge(publisher)
                .subscribe(
                        entry -> {
                            // Get the current time again
                            Instant endTime = Instant.now();
                            // Calculate the time elapsed in milliseconds
                            var executionTime = Duration.between(startTime, endTime);
                            processorBag.emitValue(entry, Sinks.EmitFailureHandler.FAIL_FAST);
                            if (entry.getKey().equalsIgnoreCase(preferredExecutionKey) || (executionTime.compareTo(duration) > 0)) {
                                processor.emitValue(entry.getValue(), Sinks.EmitFailureHandler.FAIL_FAST);
                            }
                        },
                        error -> {
                            processor.tryEmitError(new Exception(error));
                        },
                        () -> {
                            processor.tryEmitEmpty();
                        });
        return (CompletableFuture<T>) processor.asMono().toFuture();
    }
}
