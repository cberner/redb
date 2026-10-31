# redb implementation decisions

This document serves as a record of implementation decisions, beyond [design.md](./design.md) that
apply to the code base:

### Why is `Value::from_bytes()` infallible?

redb guarantees that the slice passed to `from_bytes()` is the result of `as_bytes()` on the same
type -- this guarantee will not be removed because that would place unnecessary complexity on
implementors of `Value`. Therefore, any custom type which encounters a decoding failure is assumed
to be buggy, and the user is responsible for fixing their code.

For cases where a custom type absolutely must handle decoding failures -- such as an enum which
needs to support forward compatibility -- the recommended pattern is to decode into a variant such
as `Unknown`.
