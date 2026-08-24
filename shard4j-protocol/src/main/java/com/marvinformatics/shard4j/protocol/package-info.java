/**
 * Wire records shared by the coordinator and the engine, plus the two identity
 * derivations they both depend on.
 *
 * <p>This package has an empty runtime dependency tree and is kept that way by an
 * enforcer rule. It holds wire types and identity functions only: never a config key,
 * never a hostname, never a storage path, never a default.
 */
package com.marvinformatics.shard4j.protocol;
