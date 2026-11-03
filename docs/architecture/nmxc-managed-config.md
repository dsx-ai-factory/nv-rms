# Managed NMX-C static configuration

The internal `libnmxc::managed_config` helper reconciles settings owned by an RMS
caller. No RPC uses the helper yet. Existing configuration workflows are unchanged.

## Targets

`ManagedConfig` accepts a map keyed by file basename and configuration key.
`Some(value)` requests a literal value; `None` requests removal. A value of `"0"`
is a value to set, not a removal. Unlisted keys are not owned by the caller.

Construction validates all targets before controller I/O. File names use the
shared RMS non-hidden basename validator: names must be nonempty, must not start
with a dot, and may contain only ASCII letters, digits, `_`, `-`, and `.`.
File names and keys are case-sensitive. Keys are sent unchanged to the controller;
updates, removals, and readback require the exact spelling supplied by the caller.
For example, `log_flags` and `LOG_FLAGS` are distinct keys.
Values must be nonempty literals without surrounding whitespace, quotes,
escaping, comments, or control characters. An empty map requires no I/O.

## Reconciliation

1. Read every requested file completely. Missing files, duplicate files, duplicate
   directives, and ambiguous syntax fail the operation before writes.
1. For each file containing a key to remove, read again immediately before editing.
   Remove only records targeted for removal and preserve every other byte from that read.
1. Write the replacement file and verify its complete readback. The controller may
   reorder records, but record text and multiplicity must match.
1. Use the controller's key/value API to set changed or missing values. File
   replacement does not rewrite or append settings, so their syntax stays under
   the controller's control.
1. Read back all requested keys and verify their values or absence.

A failed write acknowledgement does not establish whether the controller applied
the write. Readback determines success. A successful return reports whether a
write was attempted; the boolean is not proof that a device mutation occurred.
Repeating reconciliation against matching configuration performs reads but no
writes.

`read_config` and `config_matches` also support checking a target without writing.
A snapshot records absence only for keys requested during that read. A snapshot
cannot prove that an unrequested key is absent.

## Caller responsibilities

The caller chooses the owned keys and desired values, serializes controller
mutations, and handles controller isolation and activation. The helper does not
discover devices, choose configuration policy, reserve resources, restart
controllers, or verify fabric readiness.

The operation is not transactional. Cancellation or an error can leave completed
writes in place. Recovery must read current configuration before deciding what to
restore. Fresh reads reduce stale replacement risk but do not prevent concurrent
writers; callers must coordinate access.

Errors do not include configuration values or raw controller error messages.
Parser and fake-controller tests use synthetic configuration names and values.
