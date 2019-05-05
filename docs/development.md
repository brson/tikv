# TiKV Development Guide

This guide is for those who want to develop and contribute to [TiKV]. It covers
building, running with [PD] and [TiDB], testing, benchmarking, and fuzzing,
among other things. For details about the process of contributing to
TiKV see [CONTRIBUTING.md](../CONTRIBUTING.md). A similar [guide exists
for TiDB][tidb-guide].

[Basic build and test instructions][basic] are in the README.md.

[TiKV]: https://github.com/tikv/tikv
[PD]: https://github.com/pingcap/pd
[TiDB]: https://github.com/pingcap/tidb
[tidb-guide]: https://github.com/pingcap/docs/blob/master/dev-guide/development.md
[basic]: ../README.md#building-tikv

_Note: because TiKV was originally developed as the storage layer for
TiDB, testing with TiDB remains a critical part of TiKV development,
and some of the documentation below defers to TiDB documentation._

- [Platform support](#platform-support)
- [Building and testing TiKV](#building-and-testing-tikv)
- [Navigating the source code](#navigating-the-source-code)
- [Managing TiKV compile times](#managing-tikv-compile-times)
- [Working with PD and TiDB](#working-with-pd-and-tidb)
- [Continuous Integration](#continuous-integration)
- [Benchmarking](#benchmarking)
- [Integration testing and benchmarking with TiDB](#integration-testing-and-benchmarking-with-tidb)
- [Fuzzing](#fuzzing)
- [Working with the allocator](#working-with-the-allocator)
- [Additonal maintenance considerations](#maintenance-considerations)


## Platform support

TiKV is actively developed for x86-64 on Linux and Mac. Other platforms probably
do not work, but patches for other platforms are welcome. For details, TiKV's
platform support is [similar to TiDB's][tidb-plat].

[tidb-plat]: https://github.com/pingcap/docs/blob/master/dev-guide/requirements.md#supported-platforms

## Building and testing TiKV

[Basic build instructions][basic] are in the README.md.



## Navigating the source code

TiKV is a single workspace containing a number of sub-projects. The main
Cargo.toml defines the TiKV library, which is a dependency of multiple binaries.
The library in turn depends on additional crates located in the `components`
subdirectory. Unit tests are written inline, in the modules they test.
Integration tests are written in their own crates in the `components` directory.

Below is an outline of the directory structure and TODO

- `benches` &mdash; benchmarks. These are in subdirectories and are named
  explicitly in Cargo.toml.

- `components` &mdash; sub-crates of the TiKV library. These are not generally
  useful outside of TiKV and are not published to crates.io. The `test_` crates
  are used as dev-dependencies for tests in `tests/integrations`.

- `docs` &mdash;

- `etc` &mdash; configuration files that don't belong elsewhere.

- `fuzz` &mdash; the fuzzer driver, implementations for multiple fuzzers, and
  shared fuzz targets. See the [README][fuzz-readme] in that directory for
  details.

- `images` &mdash; images embedded in the Markdown documentation.

- `tests`

  - `failpoints` - tests that use [failpoints]. These are in their own crate
    because failpoint tests must be run without parallelism to avoid accidentally
	interacting with non-failpoint tests.

  - `integrations` - integration tests.

[failpoints]: https://github.com/pingcap/fail-rs


## Managing TiKV compile times


## Working with PD and TiDB

In addition to the TiKV prerequisites noted there, you will also need any [TiDB prerequisites][tidb-req].

[tidb-req]: https://github.com/pingcap/docs/blob/master/dev-guide/requirements.md#prerequisites


## Continuous integration

TiKV uses [Jenkins] for continuous integration. It runs against every pull
request automatically and must pass prior to merging. Maintainers with
appropriate access may control the CI system by issuing it commands as comments
to PRs.

- `/test

For contributors with read-only access, if you need the CI re-run, ask in a
comment on the PR while @-mentioning one of the reviewers.

Note that the test suite contains tests that fail spuriously, so it is
unfortunately common that CI must be run multiple times per PR. When these
spurious tests are seen, the issue tracker should be searched for open issues
containing the name of the test, and, if none exist, issues should be filed and
tagged `C: Test/Bench` + `T: Bug`.

The CI build is heavily customized to run tests distributed across a cluster of
machines, so there is no single script or Makefile target that exactly
reproduces the CI behavior. The `ci-build` Makefile target though does though
perform the same build as the CI and produces the metadata used to distribute
the test suite.

Although there is a `Jenkinsfile` in the repository,
the entire CI configuration presently lives in a private PingCAP repo. To change
the CI please file issues against the `tikv` repo.

[Jenkins]: https://jenkins.io/


## Fuzzing

The `fuzz` project, accessed with `cargo -p fuzz` is used to fuzz TiKV with each
of [AFL], [Honggfuzz], and [libfuzzer]. See the fuzzing [README][fuzz-readme]
for details.

[AFL]: http://lcamtuf.coredump.cx/afl/
[Honggfuzz]: http://honggfuzz.com/
[libfuzzer]: http://llvm.org/docs/LibFuzzer.html
[fuzz-readme]: fuzz/README.md


## Additional maintenance considerations

- Crates that do not contain tests should set `test = false` in their manifest to reduce testing time and output spew.
- All crates that contain tests _but do not link to the tikv crate_ should also link to `tikv_alloc`. This is because `tikv_alloc` activates the jemalloc global allocator, which is the one TiKV uses in production.
- When adding a new dependency, if that dependency carries any cargo features, then set `default-features = false` in the manifest and explicitly enable the features you need. This reduces the amount of code cargo needs to download and build.
- New benchmark crates should use [criterion], not Rust's built-in benchmarker.
- Unit tests belong inside the modules they test.

[criterion]: https://github.com/bheisler/criterion.rs


<!--

TODO:

- should the `tests/` directory not be used?
- what's the distinction between `Dockerfile` and the dockerfiles in `docker`?
- CI commands
- who exactly can issue CI commands?
- does CI really run outomatically or does it require /ok-to-test?

-->
