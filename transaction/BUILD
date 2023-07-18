exports_files(["speedbjni-2.5.0-linux64.jar"])
java_library(
    name = "transaction",
    srcs = [
        "RocksDatabase.java",
        "RocksTransaction.java",
    ],
    deps = [
        ":speedbjni-2.5.0-linux64.jar",
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
        ":speedbjni-2.5.0-linux64.jar",
    ],
)
