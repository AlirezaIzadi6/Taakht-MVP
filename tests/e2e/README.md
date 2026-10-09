# End-to-end tests

Drive the running stack (`make dev`) over gRPC. They run only with `E2E=1` (`make e2e` sets it and uses `-count=1`).

Two modes, selected by how the swap service was started:

- Default (long payment deadline, `PAYMENT_DEADLINE` unset or large): runs `TestAdMatchingOnly`, `TestHappyPath`,
  `TestLockRace`, `TestLockerEligibility`. `TestPaymentTimeout` is skipped.
- Short deadline: start swap with `PAYMENT_DEADLINE=5s` and run with `E2E_SHORT_DEADLINE=1`. Only
  `TestPaymentTimeout` runs; the four tests above skip themselves because they would race the timeout.

Test cleanup hides any ad still PUBLISHED when a test ends (ads that were LOCKED get a short grace period to settle)
and never fails a test.
