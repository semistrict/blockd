---
status: accepted
---

# Object-store CAS is the control plane

The object store is the system's sole shared authority for cluster membership
and control-plane state. Every join, renewal, removal, placement, session,
authority, per-volume head, and replica-assignment transition uses a conditional
operation against the exact observed object version; peers exchange data and
health observations but never decide control state through a quorum or a second
consensus mechanism. While the object store is unavailable, nodes may continue
data-plane work only under already-authorized fenced state and must stop when its
bounded validity expires; no new control-plane transition is allowed. We accept
that loss of the object store also removes control-plane availability in order to
keep split-brain resolution in one durable mechanism.

There is one durable `cluster/placement` object. Its roster is the membership
snapshot used both to choose per-volume authority and to rank passive replica
destinations; those are derived views, not independently writable control
planes. A membership change is visible only after one version-checked write of
that object succeeds.
