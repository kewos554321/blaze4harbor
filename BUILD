load("//devtools/python/blaze:pytype.bzl", "pytype_strict_binary", "pytype_strict_library")

pytype_strict_library(
    name = "gcs_upload_lib",
    srcs = ["gcs_upload.py"],
)

pytype_strict_binary(
    name = "gcs_upload_cli",
    srcs = ["gcs_upload.py"],
    main = "gcs_upload.py",
)

pytype_strict_binary(
    name = "blaze4harbor",
    srcs = ["main.py"],
    main = "main.py",
    deps = [":gcs_upload_lib"],
)