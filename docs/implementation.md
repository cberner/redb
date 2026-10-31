# redb implementation decisions

This document serves as a record of implementation decisions, beyond [design.md](./design.md) that
apply to the code base:

### Why is `Value::from_bytes()` infallible?

redb guarantees that, with an intact database file, the slice passed to `from_bytes()` is the result
of `as_bytes()` on the same type -- this guarantee will not be removed because that would place
unnecessary complexity on implementors of `Value`. Therefore, any custom type which encounters a
decoding failure is assumed to be buggy, and the user is responsible for fixing their code.

For cases where a custom type absolutely must handle decoding failures -- such as an enum which
needs to support forward compatibility -- the recommended pattern is to decode into a variant such
as `Unknown`.

Bitrot and other external corruption are outside the design's media assumptions
(see [assumptions about underlying media](./design.md#assumptions-about-underlying-media))
and are not detected during normal reads. `check_integrity()` verifies every page checksum and can
be used as a point-in-time check when corruption is suspected. The checksums are xxh3, which is
sufficient to detect accidental corruption but not deliberate tampering; users who need tamper
evidence should layer their own integrity check.
