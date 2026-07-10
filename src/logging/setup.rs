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

use crate::logging::logfmt;
use eyre::WrapErr;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use tracing_subscriber::prelude::*;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{Layer, reload};

pub fn dep_log_filter(env_filter: EnvFilter) -> EnvFilter {
    const DEPS: &str = "sqlxmq::runner=warn,sqlx::query=warn,\
        sqlx::extract_query_data=warn,rustify=off,hyper=error,\
        rustls=warn,tokio_util::codec=warn,vaultrs=error,h2=warn";

    let user = env_filter.to_string();
    let combined = if user.is_empty() {
        DEPS.to_string()
    } else {
        format!("{DEPS},{user}")
    };

    EnvFilter::builder()
        .parse(&combined)
        .unwrap_or_else(|err| panic!("could not reparse combined filter '{combined}': {err}"))
}

pub fn setup_logging(extra_logfmt_event_fields: Vec<String>) -> eyre::Result<()> {
    let log_level = LevelFilter::INFO.into();

    // We set up some global filtering using `tracing`s `EnvFilter` framework
    // The global filter will apply to all `Layer`s that are added to the
    // `logging_subscriber` later on. This means it applies for both logging to
    // stdout as well as for OpenTelemetry integration.
    // We ignore a lot of spans and events from 3rd party frameworks
    let initial_log_filter = EnvFilter::builder()
        .with_default_directive(log_level)
        .from_env()?;
    let initial_log_filter = dep_log_filter(initial_log_filter);

    let (logfmt_stdout_filter, _logfmt_stdout_reload_handle) =
        reload::Layer::new(initial_log_filter.clone());
    let logfmt_stdout_formatter = logfmt::layer().with_event_fields(extra_logfmt_event_fields);

    tracing_subscriber::registry()
        .with(logfmt_stdout_formatter.with_filter(logfmt_stdout_filter))
        .try_init()
        .wrap_err("new tracing subscriber try_init()")?;

    tracing::info!("Current log level: {}", LevelFilter::current());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Install `dep_log_filter(user_directives)` as the thread-local subscriber
    /// for the duration of `f`, so `tracing::enabled!` calls inside reflect the
    /// effective filter.
    fn with_filter<R>(user_directives: &str, f: impl FnOnce() -> R) -> R {
        use tracing_subscriber::prelude::*;

        let user = EnvFilter::builder().parse(user_directives).unwrap();
        let subscriber = tracing_subscriber::registry().with(dep_log_filter(user));
        tracing::subscriber::with_default(subscriber, f)
    }

    #[test]
    fn user_directives_override_defaults() {
        with_filter("info,vaultrs=debug,rustify=trace", || {
            assert!(
                tracing::enabled!(target: "vaultrs", tracing::Level::DEBUG),
                "user's vaultrs=debug should win over the dep cap"
            );
            assert!(
                tracing::enabled!(target: "rustify", tracing::Level::TRACE),
                "user's rustify=trace should win over rustify=off"
            );
            // Unspecified dep target still capped at error.
            assert!(
                !tracing::enabled!(target: "hyper", tracing::Level::INFO),
                "hyper should still be capped at error by dep default"
            );
        });
    }

    #[test]
    fn bare_default_does_not_override_dep_defaults() {
        // RUST_LOG=info; user only sets a default, no per-target directives.
        with_filter("info", || {
            // User's default applies to unrelated targets.
            assert!(tracing::enabled!(target: "carbide", tracing::Level::INFO));
            assert!(!tracing::enabled!(target: "carbide", tracing::Level::DEBUG));
            // Dep caps still apply where the user didn't override.
            assert!(!tracing::enabled!(target: "hyper", tracing::Level::INFO));
            assert!(tracing::enabled!(target: "hyper", tracing::Level::ERROR));
            assert!(!tracing::enabled!(target: "vaultrs", tracing::Level::INFO));
            assert!(tracing::enabled!(target: "vaultrs", tracing::Level::ERROR));
        });
    }

    #[test]
    fn user_target_overrides_default_without_touching_others() {
        // RUST_LOG=info,carbide=debug; raises one target; deps stay capped.
        with_filter("info,carbide=debug", || {
            assert!(tracing::enabled!(target: "carbide", tracing::Level::DEBUG));
            // Unrelated target still at the INFO default.
            assert!(tracing::enabled!(target: "other", tracing::Level::INFO));
            assert!(!tracing::enabled!(target: "other", tracing::Level::DEBUG));
            // Dep caps unaffected.
            assert!(!tracing::enabled!(target: "hyper", tracing::Level::INFO));
        });
    }

    #[test]
    fn unmentioned_dep_default_stays_when_user_raises_another() {
        // User raises vaultrs but says nothing about hyper, hyper stays default.
        with_filter("info,vaultrs=trace", || {
            assert!(tracing::enabled!(target: "vaultrs", tracing::Level::TRACE));
            assert!(!tracing::enabled!(target: "hyper", tracing::Level::INFO));
            assert!(tracing::enabled!(target: "hyper", tracing::Level::ERROR));
        });
    }

    #[test]
    fn regression_debug_default_directive_survives_dep_filter() {
        // Make sure with_default_directive is not ignored
        let initial = EnvFilter::builder()
            .with_default_directive(LevelFilter::DEBUG.into())
            .parse("")
            .unwrap();

        let subscriber = tracing_subscriber::registry().with(dep_log_filter(initial));
        tracing::subscriber::with_default(subscriber, || {
            assert!(tracing::enabled!(target: "carbide", tracing::Level::DEBUG));
            assert!(!tracing::enabled!(target: "carbide", tracing::Level::TRACE));
        });
    }
}
