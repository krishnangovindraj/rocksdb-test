java_library(
    name = "transaction",
    srcs = [
        "RocksDatabase.java",
        "RocksTransaction.java",
    ],
    deps = [
        "//dependencies/maven/artifacts/org/rocksdb:rocksdbjni"
    ],
    visibility = ["//visibility:public"],
)

java_test(
    name = "test",
    srcs = [
        "TransactionTest.java"
    ],
    test_class = "rocksdbtest.transaction.TransactionTest",
    deps = [
        ":transaction",
        "//dependencies/maven/artifacts/org/rocksdb:rocksdbjni"
    ],
)
