java_binary(
    name = "testdb",
    srcs = glob(["*.java"]),
    main_class = "grakn.rocksdbtest.TestDB",
    deps = [
        "//graphdb",
        "//dependencies/maven/artifacts/org/rocksdb:rocksdbjni",
    ],
)