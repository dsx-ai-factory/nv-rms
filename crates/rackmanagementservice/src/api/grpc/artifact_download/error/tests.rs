// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn status_mapping_distinguishes_transient_and_permanent_failures() {
    for (status, expected) in [
        (400, JobError::ClientError),
        (401, JobError::Unauthenticated),
        (403, JobError::Unauthenticated),
        (404, JobError::FileNotFound),
        (408, JobError::Timeout),
        (429, JobError::ServerError),
        (503, JobError::ServerError),
    ] {
        let error = ArtifactError::HttpStatus {
            status: reqwest::StatusCode::from_u16(status).unwrap(),
            url: "https://example.test/file".to_owned(),
        };
        assert_eq!(error.job_error(), expected);
    }
    assert_eq!(
        ArtifactError::Io {
            context: "write cache".to_owned(),
            source: std::io::ErrorKind::PermissionDenied.into()
        }
        .job_error(),
        JobError::FailedPrecondition
    );
    assert_eq!(
        ArtifactError::Integrity("checksum mismatch".to_owned()).job_error(),
        JobError::FailedPrecondition
    );
}

#[test]
fn only_failures_another_attempt_can_cure_are_transient() {
    let status = |code| ArtifactError::HttpStatus {
        status: reqwest::StatusCode::from_u16(code).unwrap(),
        url: "https://example.test/file".to_owned(),
    };
    for (code, transient) in [
        (400, false),
        (401, false),
        (404, false),
        (408, true),
        (429, true),
        (500, true),
        (503, true),
    ] {
        assert_eq!(status(code).is_transient(), transient, "{code}");
    }
    assert!(status(503).context("HMC".to_owned()).is_transient());
    assert!(!ArtifactError::Integrity("checksum mismatch".to_owned()).is_transient());
}

#[test]
fn a_permanent_batch_failure_is_not_hidden_by_a_transient_one() {
    for reverse in [false, true] {
        let mut errors = vec![
            ArtifactError::HttpStatus {
                status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
                url: "https://example.test/file".to_owned(),
            },
            ArtifactError::Integrity("checksum mismatch".to_owned()).context("HMC".to_owned()),
        ];
        if reverse {
            errors.reverse();
        }
        let error = ArtifactError::Multiple(errors);
        assert_eq!(error.job_error(), JobError::FailedPrecondition);
        assert!(error.to_string().contains("HMC: checksum mismatch"));
        assert!(error.to_string().contains("503"));
    }
}

#[tokio::test]
async fn an_io_timeout_is_transient_without_being_a_connect_error() {
    let server = wiremock::MockServer::start().await;
    let body = reqwest::Body::wrap_stream(futures::stream::once(async {
        Err::<Vec<u8>, _>(std::io::Error::from(std::io::ErrorKind::TimedOut))
    }));
    let source = reqwest::Client::new()
        .post(server.uri())
        .body(body)
        .send()
        .await
        .unwrap_err();
    assert!(source.is_timeout());
    assert!(!source.is_connect());

    let error = ArtifactError::from(source);
    assert_eq!(error.job_error(), JobError::Timeout);
    assert!(error.is_transient());
}

#[tokio::test]
async fn transport_error_text_does_not_include_url_credentials_or_query() {
    let server = wiremock::MockServer::start().await;
    let mut url = reqwest::Url::parse(&server.uri()).unwrap();
    url.set_username("user").unwrap();
    url.set_password(Some("password")).unwrap();
    url.set_query(Some("token=secret-token"));
    let source = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap_err();
    let error = ArtifactError::from(source);
    let text = format!("{error} {error:?}");
    assert!(!text.contains("password"), "{text}");
    assert!(!text.contains("secret-token"), "{text}");
}
