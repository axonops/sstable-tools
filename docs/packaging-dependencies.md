# Thin JAR Packaging Dependencies

This document is the dependency and relocation record for the version-specific
workspace artifacts and their Linux packages.

## Packaged code

Each `sstable-tools-cassandra-<line>.jar` contains only:

- the project-owned `bootstrap` classes;
- the project-owned `workspace-core` classes;
- the project-owned `worker-api` classes;
- exactly one project-owned release adapter;
- Gson, relocated from `com.google.gson` to
  `com.axonops.sstable.internal.gson`, for strict manifest JSON parsing; and
- the adapter metadata and executable manifest.

Gson `2.14.0` is the only packaged third-party library. It is used because Java
8 does not provide a standard JSON parser, is released under Apache License 2.0,
and supports Java 8. No other third-party libraries are packaged. When another
tool-owned dependency is required, its coordinates, reason, relocated namespace,
and license must be added here before it is allowed by packaging verification.

## Provided runtime

`cassandra-all` and all Cassandra-supplied runtime dependencies are declared
with Maven `provided` scope. They are loaded only in the worker child process
from the Cassandra installation selected by the bootstrap. They are never
shaded into the tool artifact.

The Cassandra 3.11 integration fixture compiler additionally declares stable
`com.datastax.cassandra:cassandra-driver-core:3.0.1` with Maven `test` scope.
Cassandra's `CQLSSTableWriter` exposes driver UDT and tuple value types, but the
published `cassandra-all` POM does not supply those classes transitively. The
live fixture process uses the matching driver JAR already present in the
3.11.19 installation; packaging verification proves it is absent from the
distributed thin JAR.

The compile pins are:

| Adapter | Provided Cassandra dependency | Class-file target |
|---|---:|---:|
| 3.11 | 3.11.19 | Java 8 (52) |
| 4.0 | 4.0.0 | Java 8 (52) |
| 4.1 | 4.1.11 | Java 11 (55) |
| 5.0 | 5.0.4-5.0.8 | Java 17 (61) |

The 4.1 adapter includes both the legacy `long` and current
`Dispatcher.RequestTime` `QueryHandler` entry points. Runtime preflight verifies
that the handler implements the ABI exposed by the selected installation.

## Enforcement and reports

`scripts/verify-thin-jars` fails the Maven `verify` phase when an artifact:

- contains `org/apache/cassandra/**`;
- contains any namespace or resource outside the project-owned allowlist;
- omits the bootstrap or worker entry point;
- omits the relocated Gson runtime;
- uses the wrong release adapter class-file version; or
- cannot run bootstrap help, version, workspace create, and workspace status
  without Cassandra.

The CI workflow runs the same verification and generates Maven runtime
dependency trees for every reactor module. The release workflow also scans the
source dependency graph and published SPDX SBOM with Trivy, retaining complete
JSON results and the Maven dependency trees in the versioned security report
archive. The full source graph is report-only because it contains compatibility
and provided dependencies that are not packaged. HIGH or CRITICAL findings in
the published SBOM block release publication.

## DEB and RPM payload

The architecture-independent `sstable-tools` DEB and noarch RPM contain the
four already-verified thin JARs, one auto-detecting launcher, four explicit
release-specific launchers, the README, license, compatibility manifest, SPDX
SBOM, third-party notices, and checksums. They do not add Cassandra classes or
any other runtime library.

The package declares a Java 17-or-newer headless runtime because the combined
package exposes the Cassandra 5.0 adapter. Cassandra itself remains an external,
operator-selected runtime. The package build compares its installed JARs
byte-for-byte with the release bundle and executes every extracted launcher
during verification.
