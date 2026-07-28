# Releasing SSTable Tools

Published versions are derived from Git tags. The tag `v1.2.3` produces tool
version `1.2.3`; no separate release-version file is maintained. The Maven
`revision` property remains `0.1.0-SNAPSHOT` for ordinary development builds,
and the release workflow overrides it with the validated tag version.

## Automated tagged release

Create and push an annotated `vMAJOR.MINOR.PATCH` tag at the commit to release:

```shell
git tag -a v0.1.0 -m "SSTable Tools 0.1.0"
git push origin v0.1.0
```

The `Release` GitHub Actions workflow:

1. removes the `v` prefix and validates the version;
2. builds and tests the reactor with that exact Maven revision;
3. verifies that every JAR reports the same embedded version;
4. creates the release bundle, DEB, and RPM;
5. extracts and verifies both packages;
6. writes final SHA-256 checksums and optionally signs them; and
7. uploads the directory as an immutable workflow artifact and a GitHub
   Release.

The workflow can also be dispatched manually with an explicit version. This is
intended for release testing or recovery; tagged releases remain the normal
path and source of truth.

## Artifacts

For version `1.2.3`, the published directory contains:

```text
sstable-tools-cassandra-3.11-1.2.3.jar
sstable-tools-cassandra-4.0-1.2.3.jar
sstable-tools-cassandra-4.1-1.2.3.jar
sstable-tools-cassandra-5.0-1.2.3.jar
sstable-tools
sstable-tools-cassandra-3.11
sstable-tools-cassandra-4.0
sstable-tools-cassandra-4.1
sstable-tools-cassandra-5.0
sstable-tools_1.2.3-1_all.deb
sstable-tools-1.2.3-1.noarch.rpm
compatibility-manifest.json
sbom.spdx.json
THIRD-PARTY-NOTICES.txt
PACKAGE-CONTENTS-SHA256SUMS
SHA256SUMS
SHA256SUMS.asc
```

The GitHub Release also contains `sstable-tools-1.2.3.tar.gz`, a reproducible
standalone archive of that directory which preserves executable launcher modes.

`SHA256SUMS.asc` is present only when `RELEASE_GPG_PRIVATE_KEY` is configured
for the repository. `PACKAGE-CONTENTS-SHA256SUMS` is embedded in the DEB and
RPM and covers their JARs and metadata. Final `SHA256SUMS` additionally covers
both package archives.

The package revision defaults to `1`. For a packaging-only rebuild of the same
upstream version, pass `--package-release 2` (or the next positive integer) to
`scripts/package-linux-packages`; do not create a different tool version.

## Local reproduction

The workflow pins nFPM `v2.47.0`. With Java 17, nFPM, `dpkg-deb`, `rpm`,
`rpm2cpio`, `cpio`, and GNU tar installed:

```shell
VERSION=0.1.0
mvn clean verify -Drevision="$VERSION"
scripts/package-release --output target/release --version "$VERSION"
scripts/package-linux-packages \
  --release-dir "target/release/sstable-tools-$VERSION"
scripts/verify-linux-packages "target/release/sstable-tools-$VERSION"
scripts/verify-release-bundle "target/release/sstable-tools-$VERSION"
```

Pass `--sign` to `scripts/package-linux-packages` only after the appropriate
GPG private key is available. Signing is deliberately the last packaging step
because the Linux packages are included in the final checksum file.
