load("//devtools/python/blaze:pytype.bzl", "pytype_strict_binary")

pytype_strict_binary(
    name = "blaze4harbor",
    srcs = ["main.py"],
    main = "main.py",
)

pytype_strict_binary(
    name = "bigquery_upload",
    srcs = ["bigquery_upload.py"],
    main = "bigquery_upload.py",
    deps = [
        "//third_party/py/google/cloud:core",
        "//third_party/py/google/cloud/bigquery",
    ],
)

pytype_strict_binary(
    name = "gcs_upload",
    srcs = ["gcs_upload.py"],
    main = "gcs_upload.py",
    deps = ["//third_party/py/google/cloud/storage"],
)