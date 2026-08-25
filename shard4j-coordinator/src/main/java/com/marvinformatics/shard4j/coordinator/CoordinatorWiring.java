package com.marvinformatics.shard4j.coordinator;

import com.marvinformatics.shard4j.coordinator.core.CoordinatorCore;
import com.marvinformatics.shard4j.coordinator.storage.DataDirectory;
import com.marvinformatics.shard4j.coordinator.storage.DurationStore;
import com.marvinformatics.shard4j.coordinator.storage.HistoryLog;
import com.marvinformatics.shard4j.coordinator.storage.SessionLog;
import com.marvinformatics.shard4j.coordinator.web.SecretAuthFilter;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

/**
 * Boot order is the durability argument: refuse-to-start checks, then the exclusive
 * directory lock and the fsynced incarnation bump, then snapshot load or cold seed fold,
 * then session-log replay -- all before the web listener can serve a mutating request.
 */
@Slf4j
@Configuration
public class CoordinatorWiring {

  @Bean(destroyMethod = "close")
  public DataDirectory dataDirectory(CoordinatorSettings settings) {
    settings.requireCompleteness();
    log.info("Accepting {} shared secret value(s)", settings.secrets().size());
    return DataDirectory.open(settings.dataDir(), settings.tenantSlug());
  }

  @Bean(destroyMethod = "close")
  public SessionLog sessionLog(DataDirectory dataDirectory) {
    return new SessionLog(dataDirectory.sessionsDir());
  }

  @Bean(destroyMethod = "close")
  public HistoryLog historyLog(DataDirectory dataDirectory) {
    return new HistoryLog(dataDirectory.historyDir());
  }

  @Bean
  public DurationStore durationStore(
      DataDirectory dataDirectory, HistoryLog historyLog, CoordinatorSettings settings) {
    DurationStore store =
        new DurationStore(dataDirectory.snapshotFile(), settings.durationClamp().toMillis());
    Instant now = Clock.systemUTC().instant();
    if (!store.loadSnapshot()) {
      store.coldLoad(historyLog.readWithin(settings.historyRetention(), now));
      if (store.isEmpty()) {
        log.error(
            "Duration store is empty for {}; ordering degrades to hash order until history"
                + " accrues. A deployment that expected a seed should check its data volume.",
            dataDirectory.tenantDir());
      } else {
        store.saveSnapshot();
      }
    }
    return store;
  }

  @Bean
  public CoordinatorCore coordinatorCore(
      DataDirectory dataDirectory,
      SessionLog sessionLog,
      HistoryLog historyLog,
      DurationStore durationStore,
      CoordinatorSettings settings) {
    Clock clock = Clock.systemUTC();
    Instant now = clock.instant();
    sessionLog.prune(settings.gcIdle(), now);
    historyLog.prune(settings.historyRetention(), now);
    CoordinatorCore core =
        CoordinatorCore.builder()
            .sessionLog(sessionLog)
            .historyLog(historyLog)
            .durations(durationStore)
            .clock(clock)
            .tenantKey(settings.tenantKey())
            .incarnation(dataDirectory.incarnation())
            .leaseTtl(settings.leaseTtl())
            .maxClaimBatch(settings.maxClaimBatch())
            .gcIdle(settings.gcIdle())
            .build();
    core.replay(sessionLog.replay(settings.gcIdle(), now));
    log.info("Coordinator ready at incarnation {}", dataDirectory.incarnation());
    return core;
  }

  /**
   * Boot-time pruning and idle GC alone are not enough for a long-lived process, and the
   * duration snapshot is debounced out of the result path -- so both run here on a clock.
   * Every task swallows its own failures: maintenance must never kill its executor.
   */
  @Bean(destroyMethod = "shutdownNow")
  public ScheduledExecutorService maintenanceScheduler(
      SessionLog sessionLog,
      HistoryLog historyLog,
      DurationStore durationStore,
      CoordinatorCore core,
      CoordinatorSettings settings) {
    ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "coordinator-maintenance");
              thread.setDaemon(true);
              return thread;
            });
    scheduler.scheduleWithFixedDelay(
        () -> {
          try {
            Instant now = Clock.systemUTC().instant();
            sessionLog.prune(settings.gcIdle(), now);
            historyLog.prune(settings.historyRetention(), now);
            core.gcIdleSessions();
          } catch (RuntimeException e) {
            log.warn("Scheduled maintenance failed; will retry next cycle: {}", e.toString());
          }
        },
        1,
        1,
        TimeUnit.HOURS);
    scheduler.scheduleWithFixedDelay(
        () -> {
          try {
            durationStore.saveIfDirty();
          } catch (RuntimeException e) {
            log.warn("Duration snapshot flush failed; will retry next cycle: {}", e.toString());
          }
        },
        30,
        30,
        TimeUnit.SECONDS);
    return scheduler;
  }

  @Bean
  public FilterRegistrationBean<SecretAuthFilter> secretAuthFilter(CoordinatorSettings settings) {
    FilterRegistrationBean<SecretAuthFilter> registration =
        new FilterRegistrationBean<>(
            new SecretAuthFilter(settings.secrets(), settings.publicRead()));
    registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
    return registration;
  }
}
