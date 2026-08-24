/**
 * The single-instance scheduling service.
 *
 * <p>Its environmental demands are deliberately tiny: a TCP port, one writable directory
 * and a wall clock. No database, no cloud API, no outbound network, no DNS. It contains no
 * deployment information: no hostname, no cluster, no namespace, no registry. Where an
 * instance runs is entirely a deployer's business.
 */
package com.marvinformatics.shard4j.coordinator;
