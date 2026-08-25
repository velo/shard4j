package com.marvinformatics.shard4j.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.marvinformatics.shard4j.protocol.BarrierRequest;
import com.marvinformatics.shard4j.protocol.BarrierResponse;
import com.marvinformatics.shard4j.protocol.ClaimRequest;
import com.marvinformatics.shard4j.protocol.DepartRequest;
import com.marvinformatics.shard4j.protocol.Fence;
import com.marvinformatics.shard4j.protocol.Grant;
import com.marvinformatics.shard4j.protocol.NackRequest;
import com.marvinformatics.shard4j.protocol.NextClassRequest;
import com.marvinformatics.shard4j.protocol.NextClassResponse;
import com.marvinformatics.shard4j.protocol.Pass;
import com.marvinformatics.shard4j.protocol.RegisterRequest;
import com.marvinformatics.shard4j.protocol.RegisterResponse;
import com.marvinformatics.shard4j.protocol.ResultRequest;
import feign.Feign;
import feign.FeignException;
import feign.RetryableException;
import feign.Retryer;
import feign.codec.ErrorDecoder;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

/**
 * The shard's stateful view of the coordinator: it owns the registration payload so a
 * {@code 404} on any later call -- a session the coordinator forgot or never flushed -- is
 * answered by re-registering and retrying, never by failing the shard. With no heartbeat
 * in the protocol, the retry budget carried by the transport layer below is the entire
 * availability story and must exceed the coordinator deployment's restart time.
 *
 * <p>All calls are serialised: the engine is single-threaded, and the one background
 * caller -- the liveness keepalive -- must not interleave with a re-registration.
 *
 * <p>Deliberately not final: the engine's own tests override single calls to inject the
 * transport failures -- an exhausted retry budget mid-batch -- that no healthy coordinator
 * can be asked to produce.
 */
class CoordinatorGateway {

  private static final System.Logger log = System.getLogger(CoordinatorGateway.class.getName());

  private static final ObjectMapper JSON =
      JsonMapper.builder()
          .addModule(new JavaTimeModule())
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
          .build();

  private final CoordinatorClient api;
  private final ShardConfiguration configuration;
  private final RegisterRequest registration;
  private long epoch;

  CoordinatorGateway(ShardConfiguration configuration, List<String> censusUnitIds) {
    this.configuration = configuration;
    this.registration =
        new RegisterRequest(
            configuration.shardIndex(),
            configuration.attempt(),
            configuration.metadata(),
            censusUnitIds);
    this.api =
        Feign.builder()
            .encoder(new JacksonEncoder(JSON))
            .decoder(new JacksonDecoder(JSON))
            .requestInterceptor(
                template ->
                    template.header("Authorization", "Bearer " + configuration.coordinatorSecret()))
            .retryer(new BudgetedRetryer(configuration.retryBudget()))
            .errorDecoder(new RetryServerErrors())
            .target(CoordinatorClient.class, configuration.coordinatorUrl());
  }

  synchronized long register() {
    RegisterResponse response;
    try {
      response = api.register(configuration.sessionId(), registration);
    } catch (FeignException.Conflict e) {
      // A diverging census can never pass; surface the coordinator's naming of the
      // differing ids instead of a bare status code.
      throw new ShardExecutionException("Registration refused: " + e.contentUTF8());
    }
    epoch = response.epoch();
    log.log(
        System.Logger.Level.INFO,
        "Shard "
            + configuration.shardIndex()
            + " registered "
            + response.registeredCount()
            + " lease units in session "
            + configuration.sessionId()
            + " (epoch "
            + epoch
            + ")");
    return epoch;
  }

  /**
   * The open ask: the coordinator names the class this shard should drain next, first
   * batch of leases included. The shard brings no candidates -- registration already gave
   * the coordinator the census, and which class comes next is exactly the decision this
   * call exists to hand over.
   */
  synchronized NextClassResponse nextClass() {
    return withSession(
        () ->
            api.next(
                configuration.sessionId(),
                new NextClassRequest(configuration.shardIndex(), configuration.pass())));
  }

  synchronized List<Grant> claim(String className, List<String> candidates) {
    return withSession(
            () ->
                api.claim(
                    configuration.sessionId(),
                    new ClaimRequest(
                        configuration.shardIndex(), configuration.pass(), className, candidates)))
        .granted();
  }

  /**
   * An empty claim: grants nothing, mutates nothing, but counts as proof of life. The
   * coordinator presumes dead any unreleased shard that holds no lease and stays silent,
   * so the engine pings through the gaps a suite naturally has -- a slow {@code @AfterAll}
   * between classes, a long class setup before the first result.
   */
  synchronized void keepalive() {
    withSession(
        () ->
            api.claim(
                configuration.sessionId(),
                new ClaimRequest(
                    configuration.shardIndex(), configuration.pass(), "keepalive", List.of())));
  }

  /**
   * Reports one unit as it completes. A stale fence ({@code 409}) is non-fatal by design:
   * the lease was reclaimed, someone else owns the unit now, and the payload is already on
   * the coordinator's stale list -- so the unit counts as explained here.
   */
  synchronized void report(Fence fence, UnitResult result, boolean firstOnShard) {
    ResultRequest request =
        new ResultRequest(
            configuration.shardIndex(),
            configuration.pass(),
            result.unitId().value(),
            fence,
            result.outcome(),
            result.durationMs(),
            firstOnShard,
            result.reason(),
            result.invocations());
    try {
      withSession(
          () -> {
            api.result(configuration.sessionId(), request);
            return null;
          });
    } catch (FeignException.Conflict e) {
      log.log(
          System.Logger.Level.WARNING,
          "Result for "
              + result.unitId().value()
              + " was rejected as stale; the lease was reclaimed and the unit is someone"
              + " else's now");
    }
  }

  synchronized void nack(List<NackRequest.NackedLease> leases) {
    withSession(
        () -> {
          api.nack(
              configuration.sessionId(),
              new NackRequest(configuration.shardIndex(), leases));
          return null;
        });
  }

  synchronized BarrierResponse barrier(Pass completedPass) {
    return withSession(
        () ->
            api.barrier(
                configuration.sessionId(),
                new BarrierRequest(configuration.shardIndex(), epoch, completedPass)));
  }

  synchronized void depart() {
    withSession(
        () -> {
          api.depart(
              configuration.sessionId(), new DepartRequest(configuration.shardIndex(), epoch));
          return null;
        });
  }

  /**
   * The 404 fallback: sessions are created by registration only, so an unknown session on
   * a mutating call means the coordinator lost or aged it out -- and the shard is the only
   * party still carrying the census, so it re-posts it and retries.
   */
  private <T> T withSession(Supplier<T> call) {
    try {
      return call.get();
    } catch (FeignException.NotFound e) {
      log.log(
          System.Logger.Level.WARNING,
          "Session "
              + configuration.sessionId()
              + " unknown to the coordinator; re-registering and retrying");
      register();
      return call.get();
    }
  }

  /**
   * Feign retries only what is marked retryable. Connection failures already are; a 5xx
   * from a proxy in front of a restarting coordinator is the same transient and is marked
   * the same way, so the budget below governs both.
   */
  private static final class RetryServerErrors implements ErrorDecoder {

    private final ErrorDecoder base = new Default();

    @Override
    public Exception decode(String methodKey, feign.Response response) {
      Exception decoded = base.decode(methodKey, response);
      if (response.status() >= 502 && response.status() <= 504
          && !(decoded instanceof RetryableException)) {
        return new RetryableException(
            response.status(),
            "Transient upstream error " + response.status(),
            response.request().httpMethod(),
            (Long) null,
            response.request());
      }
      return decoded;
    }
  }

  /**
   * Retries with backoff until the configured budget is spent, then surfaces the failure.
   * A budget below the coordinator's restart time turns every restart into a red run, so
   * the default is generous and the knob is documented rather than hidden.
   */
  private static final class BudgetedRetryer implements Retryer {

    private final Duration budget;
    private final Instant giveUpAt;
    private long backoffMs = 250;

    BudgetedRetryer(Duration budget) {
      this.budget = budget;
      this.giveUpAt = Instant.now().plus(budget);
    }

    @Override
    public void continueOrPropagate(RetryableException e) {
      if (Instant.now().isAfter(giveUpAt)) {
        throw e;
      }
      try {
        Thread.sleep(backoffMs);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw e;
      }
      backoffMs = Math.min(backoffMs * 2, 5_000);
    }

    @Override
    public Retryer clone() {
      return new BudgetedRetryer(budget);
    }
  }
}
