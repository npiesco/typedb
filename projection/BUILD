# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test", "rustfmt_test")

package(default_visibility = [
    ":__subpackages__",
    "//server:__subpackages__",
])

rust_library(
    name = "projection",
    crate_root = "lib.rs",
    srcs = glob([
        "*.rs",
        "pgwire/*.rs",
    ]),
    deps = [
        "//encoding",

        "@crates//:sqlparser",
        "@crates//:tokio",
    ],
)

rust_test(
    name = "test_crate_projection",
    crate = ":projection",
    deps = [
        "@crates//:chrono",
        "@crates//:chrono-tz",
    ],
)

# Compares our wire encoding against bytes captured from a real Postgres 16.13.
# The golden bytes are inlined constants, so this target is hermetic.
#
# The remaining tests under tests/ drive real external clients (duckdb, dbt,
# psql) or a live Postgres on 127.0.0.1:5433. They are run with cargo, not
# Bazel, because those binaries are not part of the build graph.
rust_test(
    name = "test_pgwire_golden",
    srcs = ["tests/pgwire_golden.rs"],
    deps = [":projection"],
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":projection",
        ":test_crate_projection",
    ],
    size = "small",
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        "pgwire/*",
        "tests/*",
    ]),
    exclude = glob([
        "Cargo.*",
        "tests/pg_golden_bytes.json",
    ]),
    license_type = "mpl-header",
)
