package com.example.orders;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Sharding fixture: executed only through the coordinated harness, tagged out of the
 * module's own failsafe run. Nothing here knows shard4j exists.
 */
@Tag("shard4j-fixture")
class InventoryAuditIT {

  @Test
  void countsStock() {
    assertTrue(3 > 2);
  }

  @Disabled("ledger reconciliation is disabled in this branch")
  @Test
  void reconcilesLedger() {}

  @Test
  void needsLocalWarehouse() {
    assumeTrue(false, "warehouse service is not running locally");
  }
}
