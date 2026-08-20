# Dependency policy

Every merge is gated by `cargo audit` and all four `cargo deny` checks:
advisories, licenses, bans, and sources. Dependencies must come from the
configured crates.io registry, use an allowed OSI-compatible license from
`deny.toml`, and resolve without changing `Cargo.lock`.

An advisory exception requires a checked-in `deny.toml` entry that names the
advisory, explains why the affected code is unreachable or otherwise
mitigated, identifies an owner, and includes a removal date. License and source
exceptions are not silent: they require the same documented review. The CI
tool versions and RustSec advisory-database revision are pinned in
`.github/workflows/portable.yml`. A scheduled or reviewed policy change
advances that revision; tool or advisory interpretation upgrades are ordinary
reviewed changes.
