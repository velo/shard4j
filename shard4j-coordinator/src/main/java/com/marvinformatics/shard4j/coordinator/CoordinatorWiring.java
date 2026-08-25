package com.marvinformatics.shard4j.coordinator;

import com.marvinformatics.shard4j.coordinator.core.CoordinatorCore;
import com.marvinformatics.shard4j.coordinator.storage.DataDirectory;
import com.marvinformatics.shard4j.coordinator.storage.DurationStore;
import com.marvinformatics.shard4j.coordinator.storage.HistoryLog;
import com.marvinformatics.shard4j.coordinator.storage.SessionLog;
import com.marvinformatics.shard4j.coordinator.web.SecretAuthFilter;
import java.time.Clock;
import java.time.Instant;
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
        new CoordinatorCore(
            sessionLog,
            historyLog,
            durationStore,
            clock,
            settings.tenantKey(),
            dataDirectory.incarnation(),
            settings.leaseTtl(),
            settings.maxClaimBatch(),
            settings.gcIdle());
    core.replay(sessionLog.replay(settings.gcIdle(), now));
    log.info("Coordinator ready at incarnation {}", dataDirectory.incarnation());
    return core;
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
