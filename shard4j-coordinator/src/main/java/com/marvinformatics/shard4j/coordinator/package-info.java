/**
 * The single-instance scheduling service.
 *
 * <p>It deliberately needs very little to run: one TCP port, one writable directory and a wall
 * clock. It uses no database server (the statistics database is a file in that directory), no cloud
 * API, no outbound network and no DNS. It contains no deployment information: no hostname, cluster,
 * namespace or registry. Where an instance runs is entirely the deployer's business.
 */
package com.marvinformatics.shard4j.coordinator;
