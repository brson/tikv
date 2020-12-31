# Requires Python 3.6+

import sys
import subprocess

components = [
    "backup",
    "batch-system",
    "cdc",
    "codec",
    "collections",
    "concurrency_manager",
    "configuration",
    "encryption",
    "engine_panic",
    "engine_rocks",
    "engine_test",
    "engine_traits",
    "error_code",
    "external_storage",
    "file_system",
    "into_other",
    "keys",
    "log_wrappers",
    "match_template",
    "panic_hook",
    "pd_client",
    "profiler",
    "raft_log_engine",
    "raftstore",
    "resolved_ts",
    "rusoto_util",
    "security",
    "sst_importer",
    "test_coprocessor",
    "test_pd",
    "test_raftstore",
    "test_sst_importer",
    "test_storage",
    "test_util",
    "tidb_query_aggr",
    "tidb_query_codegen",
    "tidb_query_common",
    "tidb_query_datatype",
    "tidb_query_executors",
    "tidb_query_expr",
    "tikv_alloc",
    "tikv_util",
    "tipb_helper",
    "txn_types",
]

other_crates = [
    ("cmd", "cmd"),
    ("tests", "tests"),
    ("tikv", "./"),
]

components = [(x, f"components/{x}") for x in components]
crates = components + other_crates


errors = []

def cargo_check_default():
    cargo_run_default("check", [])

def cargo_test_default():
    cargo_run_default("test", ["--no-run"])

def cargo_check_codec(codec):
    cargo_run_codec("check", [], codec)

def cargo_test_codec(codec):
    cargo_run_codec("test", ["--no-run"], codec)

def cargo_check_test_engines(test_engine):
    cargo_run_test_engines("check", [], test_engine)

    def cargo_test_test_engines(test_engine):
    cargo_run_test_engines("test", ["--no-run"], test_engine)

def cargo_run_default(cmd, extra_args):
    for (crate, _) in crates:
        args = ["cargo", cmd, "-p", crate]
        args += extra_args
        run_and_collect_errors(args)

def cargo_run_codec(cmd, extra_args, codec):
    for (crate, path) in crates:
        (has_protobuf_features, has_test_engine_features) = get_features(path)

        if not has_protobuf_features:
            continue

        args = ["cargo", cmd, "-p", crate, "--no-default-features"]
        args += extra_args
        if has_protobuf_features:
            args += ["--features", f"{codec}-codec"]
        if has_test_engine_features:
            args += ["--features", "test-engines-rocksdb"]

        run_and_collect_errors(args)

def cargo_run_test_engines(cmd, extra_args, test_engine):
    for (crate, path) in crates:
        (has_protobuf_features, has_test_engine_features) = get_features(path)

        if not has_test_engine_features:
            continue

        args = ["cargo", cmd, "-p", crate, "--no-default-features"]
        args += extra_args
        if has_protobuf_features:
            args += ["--features", "protobuf-codec"]
        if has_test_engine_features:
            args += ["--features", f"test-engines-{test_engine}"]

        run_and_collect_errors(args)

def run_and_collect_errors(args):
    global errors
    joined_args = " ".join(args)
    print(f"running `{joined_args}`")
    res = subprocess.run(args)
    if res.returncode != 0:
        errors += [joined_args]

def get_features(path):
    path = f"{path}/Cargo.toml"
    f = open(path)
    s = f.read()
    f.close()

    has_protobuf_features = "protobuf-codec" in s
    has_test_engine_features = "test-engines-rocksdb" in s

    return (has_protobuf_features, has_test_engine_features)

print()

#cargo_check_default()
#cargo_check_codec("prost")
#cargo_check_codec("protobuf")
cargo_check_test_engines("panic")
#cargo_check_test_engines("rocksdb")
#cargo_test_default()
#cargo_test_codec("prost")
#cargo_test_codec("protobuf")
cargo_test_test_engines("panic")
#cargo_test_test_engines("rocksdb")

if len(errors) == 0:
    sys.exit(0)

print()
print("errors:")
    
for error in errors:
    print(f"    {error}")

print()

sys.exit(1)
