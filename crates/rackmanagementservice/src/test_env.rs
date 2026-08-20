/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
 */

//! Test-only helpers for mutating process environment variables.
//!
//! Rust 2024 marks [`std::env::set_var`] / [`remove_var`](std::env::remove_var) as `unsafe`; all such
//! calls are centralized here under a mutex so unit tests stay free of `unsafe`.

use std::sync::{Mutex, MutexGuard, OnceLock};

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn env_lock() -> MutexGuard<'static, ()> {
    ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}

fn set_var(key: &str, value: Option<&str>) {
    // SAFETY: `ENV_LOCK` is held; only test code uses these helpers to mutate env.
    unsafe {
        match value {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
    }
}

fn restore_var(key: &str, prior: Option<String>) {
    // SAFETY: `ENV_LOCK` is held; restoring the value observed before `set_var`.
    unsafe {
        match prior {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
    }
}

struct EnvVarRestore {
    key: String,
    prior: Option<String>,
    _lock: MutexGuard<'static, ()>,
}

impl Drop for EnvVarRestore {
    fn drop(&mut self) {
        restore_var(&self.key, self.prior.take());
    }
}

/// Run `body` with `key` set to `value`, restoring the prior value afterward.
/// Pass `None` for `value` to unset `key` for the duration of `body`.
pub fn with_var<F, R>(value: Option<&str>, key: &str, body: F) -> R
where
    F: FnOnce() -> R,
{
    let lock = env_lock();
    let prior = std::env::var(key).ok();
    set_var(key, value);
    let _restore = EnvVarRestore {
        key: key.to_string(),
        prior,
        _lock: lock,
    };
    body()
}

/// Scoped env override for unit tests. Restores the prior value on drop.
///
/// ```ignore
/// with_test_env!("SOME_ENV_VAR" => "1", {
///     assert_eq!(std::env::var("SOME_ENV_VAR").as_deref(), Ok("1"));
/// });
///
/// with_test_env!("SOME_ENV_VAR", unset, {
///     assert!(std::env::var("SOME_ENV_VAR").is_err());
/// });
/// ```
#[macro_export]
macro_rules! with_test_env {
    ($key:expr => $value:expr, $($body:tt)*) => {
        $crate::test_env::with_var(Some($value), $key, || { $($body)* })
    };
    ($key:expr, unset, $($body:tt)*) => {
        $crate::test_env::with_var(None::<&str>, $key, || { $($body)* })
    };
}
