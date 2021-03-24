load(":rules.bzl", "jmh_java_benchmarks")

jmh_java_benchmarks(
    name = "byte-buffer",
    srcs = [
        "ByteBufferBenchmarks.java"
    ],
)

jmh_java_benchmarks(
    name = "rocks-operations",
    srcs = [
        "RocksOperations.java",
    ],
    deps = [
        "//transaction:transaction",
        "//dependencies/maven/artifacts/org/rocksdb:rocksdbjni"
    ]
)