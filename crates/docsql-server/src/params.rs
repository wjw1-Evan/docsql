//! Parameter binding lives in `docsql_core::bind` since the in-process FFI
//! embedding path (crates/docsql-ffi, .NET `Data Source=…` mode) must render
//! bound values through the exact same injection-safe surface as the server.
//! This module is the server's import shim only — never fork the renderer.

pub(crate) use docsql_core::bind::{bind_params, json_param_to_value};
