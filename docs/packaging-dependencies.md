# Thin JAR Packaging Dependencies

This document is the dependency and relocation record for the version-specific
workspace artifacts.

## Packaged code

Each `sstable-tools-cassandra-<line>.jar` contains only:

- the project-owned `bootstrap` classes;
- the project-owned `workspace-core` classes;
- the project-owned `worker-api` classes;
- exactly one project-owned release adapter; and
- the adapter metadata and executable manifest.

There are currently no packaged third-party libraries and therefore no package
relocations. When a future tool-owned dependency is required, its coordinates,
reason, relocated namespace, and license must be added here before it is allowed
by packaging verification.

## Provided runtime

`cassandra-all` and all Cassandra-supplied runtime dependencies are declared
with Maven `provided` scope. They are loaded only in the worker child process
from the Cassandra installation selected by the bootstrap. They are never
shaded into the tool artifact.

The compile pins are:

| Adapter | Provided Cassandra dependency | Class-file target |
|---|---:|---:|
| 3.11 | 3.11.19 | Java 8 (52) |
| 4.0 | 4.0.17 | Java 8 (52) |
| 4.1 | 4.1.3 | Java 11 (55) |
| 5.0 | 5.0.4 | Java 17 (61) |

## Enforcement and reports

`scripts/verify-thin-jars` fails the Maven `verify` phase when an artifact:

- contains `org/apache/cassandra/**`;
- contains any namespace or resource outside the project-owned allowlist;
- omits the bootstrap or worker entry point;
- uses the wrong release adapter class-file version; or
- cannot run bootstrap help and version commands without Cassandra.

The CI workflow runs the same verification and generates Maven runtime
dependency trees for every reactor module. The four thin JARs and dependency
reports are retained together as the `thin-jars-and-dependency-reports` workflow
artifact.
