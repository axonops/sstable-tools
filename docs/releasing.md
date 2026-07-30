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

Do not use **Releases > Draft a new release** to start the build. That creates
the GitHub Release before the workflow runs. The tag push above is the normal
release trigger, and the workflow creates the GitHub Release after all build,
package, verification, and security steps pass.

The `Release` GitHub Actions workflow:

1. removes the `v` prefix and validates the version;
2. builds and tests the reactor with that exact Maven revision;
3. verifies that every JAR reports the same embedded version;
4. creates the release bundle, DEB, and RPM;
5. extracts and verifies both packages;
6. writes final SHA-256 checksums and optionally signs them; and
7. scans release artifacts with ClamAV;
8. runs CodeQL source analysis plus Trivy source, configuration, secret, and
   published-SBOM dependency vulnerability scans, while recording the full
   Maven build/compatibility dependency graph; and
9. publishes the DEB and RPM to the configured Google Artifact Registry Apt
   and Yum repositories; and
10. uploads the release and its security report archive as immutable workflow
    artifacts and GitHub Release assets.

Release publication fails on malware, scanner errors, missing reports, or
HIGH/CRITICAL CodeQL, source-security, or published-SBOM findings. The full
Maven graph is report-only because it includes compatibility and provided
dependencies that are not distributed. Security reports are uploaded as a
workflow artifact even when the security gate fails, so the failure can be
diagnosed without publishing unapproved binaries.

The workflow can also be dispatched manually with an explicit version from
**Actions > Release > Run workflow**. Select `master` and enter the version
without the `v` prefix. This is intended for release testing or recovery;
tagged releases remain the normal path and source of truth.

Release publication is rerunnable. If the GitHub Release does not yet exist,
the workflow creates it from the existing remote tag. If a non-immutable
release already exists, the workflow replaces assets with matching names and
updates its title and target. Existing release notes are preserved. GitHub
immutable releases cannot be modified and fail with an explicit error instead.

## Google Artifact Registry publication

Configure this GitHub repository secret before running a release:

```text
GCP_CREDENTIALS
```

`GCP_CREDENTIALS` contains the complete Google service-account JSON key. The
service account must be able to upload Apt and Yum artifacts to these fixed
destinations:

```text
https://europe-apt.pkg.dev/projects/axonops-public/axonops-apt
https://europe-yum.pkg.dev/projects/axonops-public/axonops-yum
```

The upload runs only after package verification and the release security gate
have passed. A failed Artifact Registry upload blocks GitHub Release
publication.

## Artifacts

For version `1.2.3`, the published directory contains:

```text
sstable-tools-cassandra-3.11-1.2.3.jar
sstable-tools-cassandra-4.0-1.2.3.jar
sstable-tools-cassandra-4.1-1.2.3.jar
sstable-tools-cassandra-5.0-1.2.3.jar
sstable-tools
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
It additionally contains `sstable-tools-1.2.3-security-reports.tar.gz`, with:

```text
security-reports/SUMMARY.md
security-reports/metadata.json
security-reports/clamav-update.txt
security-reports/clamav-scan.txt
security-reports/codeql/*.sarif
security-reports/trivy-source-security.json
security-reports/trivy-source-dependencies.json
security-reports/trivy-release-dependencies.json
security-reports/maven-dependency-trees/*.txt
```

CodeQL Action `v4.36.0` and the remediated Trivy setup action `v0.2.6` are
pinned by full commit SHA. Trivy is pinned to the known-safe immutable
`v0.69.3` binary rather than a mutable `latest` reference.

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

The complete security gate additionally requires ClamAV, `jq`, Trivy `v0.69.3`,
Maven runtime dependency-tree reports, and CodeQL SARIF output. The release
workflow provisions those tools and invokes `scripts/release-security-scan`
after package verification.
