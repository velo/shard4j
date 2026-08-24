/**
 * The shard-side JUnit Platform {@code TestEngine} and its coordinator client.
 *
 * <p>This artifact lands on every consumer's test classpath, so its shipped runtime tree
 * is an allow-list enforced by the build: shard4j-protocol, JUnit Platform, Feign and one
 * JSON codec. It must work for a project that has never heard of its authors -- no
 * filename convention, no test base class, no annotation, no CI vendor, no hostname.
 */
package com.marvinformatics.shard4j.engine;
