# Fixture Catalog

Issue #9 uses `fixtures/compatibility/legacy-reader-3.11.json` as the
machine-readable catalog for checked-in immutable SSTables. Its companion
checksum inventory covers every component under the referenced fixture root.

The catalog distinguishes evidence from historical data. `ma-2-big`,
`ma-3-big`, and `mc-1-big` have direct workflow coverage. `mb-1-big` is intentionally a
partitioner-rejection fixture. The remaining checked-in descriptors are not
compatibility claims until their producer provenance and direct cqlsh contract
coverage have been recorded.

The catalog is verified by `scripts/verify-fixture-catalog`. Future fixture
entries must record the producing Cassandra patch, schema, mutation script,
partitioner, compression, expected logical results, and component hashes. The
direct contract is the only supported execution model: explicitly selected
stopped SSTables, stock cqlsh operations, sibling output, and direct reopen.
