// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::time::Duration;

#[tokio::test]
async fn cached_artifact_missing_digest_error_redacts_the_source() {
    let location = serde_json::from_value(serde_json::json!({
        "Location": "https://USER:PASSWORD@example.test/fw.bin?token=SECRET#FRAGMENT",
        "LocationType": "https"
    }))
    .unwrap();

    let artifact = Artifact::from_location("revision", "BMC".to_owned(), &location).unwrap();
    let cache = tempfile::tempdir().unwrap();

    let error = artifact
        .verify_required_http_digest(cache.path())
        .await
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "firmware artifact https://example.test/fw.bin is missing a required Sha256 digest"
    );

    assert_eq!(
        error.job_error(),
        crate::orchestrator::job_lifecycle::JobError::InvalidArgument
    );
}

#[tokio::test]
async fn optional_failures_do_not_enter_ready_or_fail_required_preparation() {
    use wiremock::{Mock, MockServer, ResponseTemplate};
    let server = MockServer::start().await;
    Mock::given(wiremock::matchers::path("/good.bin"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"firmware"))
        .mount(&server)
        .await;
    let artifact = |name: &str, required| {
        Artifact::from_location("revision", name.to_owned(),
        &serde_json::from_value(serde_json::json!({
            "Location": format!("{}/{name}", server.uri()), "LocationType": "http", "Required": required
        })).unwrap()).unwrap()
    };
    let good = artifact("good.bin", true);
    let optional = artifact("optional-missing.bin", false);
    let required = artifact("required-missing.bin", true);
    let temp = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let ready = materialize(
        &[good.clone(), optional.clone()],
        temp.path(),
        None,
        false,
        &ArtifactDownloadPolicy::default(),
        &cancel,
    )
    .await
    .unwrap();
    assert_eq!(ready, HashSet::from([good.id.clone()]));
    assert!(!optional.cache_path(temp.path()).unwrap().exists());
    let error = materialize(
        &[good.clone(), optional, required],
        temp.path(),
        None,
        false,
        &ArtifactDownloadPolicy::default(),
        &cancel,
    )
    .await
    .unwrap_err();
    let ArtifactError::Multiple(errors) = error else {
        panic!("required failure must be retained");
    };
    assert_eq!(errors.len(), 1);
    assert!(errors[0].to_string().contains("required-missing.bin"));
    assert_eq!(
        errors[0].job_error(),
        crate::orchestrator::job_lifecycle::JobError::FileNotFound
    );
    assert!(good.cache_path(temp.path()).unwrap().is_file());
}

#[tokio::test]
async fn cancelling_during_a_retry_backoff_stops_the_download() {
    use wiremock::{Mock, MockServer, ResponseTemplate};
    let server = MockServer::start().await;
    Mock::given(wiremock::matchers::method("GET"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;
    let location = serde_json::from_value(serde_json::json!({
        "Location": format!("{}/bmc.fwpkg", server.uri()), "LocationType": "http"
    }))
    .unwrap();
    let artifact = Artifact::from_location("revision", "BMC".to_owned(), &location).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let policy = ArtifactDownloadPolicy::default();
    let cancel_during_backoff = async {
        while server.received_requests().await.unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Let the 503 reach the client, so the cancel lands in the 2 s wait.
        tokio::time::sleep(Duration::from_millis(200)).await;
        cancel.cancel();
        std::time::Instant::now()
    };
    let (result, cancelled_at) = tokio::join!(
        materialize(
            std::slice::from_ref(&artifact),
            temp.path(),
            None,
            false,
            &policy,
            &cancel,
        ),
        cancel_during_backoff
    );
    assert!(matches!(result, Err(ArtifactError::Cancelled)));
    // Returning well inside the backoff shows the cancel ended the wait.
    assert!(cancelled_at.elapsed() < Duration::from_secs(1));
    server.verify().await;
}

#[tokio::test]
async fn cancelling_a_large_local_copy_removes_the_partial_file() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("large.fwpkg");
    let file = tokio::fs::File::create(&source).await.unwrap();
    // Sparse source avoids allocating a large fixture in memory or on disk.
    file.set_len(128 * 1024 * 1024).await.unwrap();
    drop(file);
    let location = serde_json::from_value(serde_json::json!({
        "Location": source, "LocationType": "file"
    }))
    .unwrap();
    let artifact = Artifact::from_location("revision", "BMC".to_owned(), &location).unwrap();
    let cache = temp.path().join("cache");
    let destination = artifact.cache_path(&cache).unwrap();
    let directory = destination.parent().unwrap();
    let cancel = CancellationToken::new();
    let policy = ArtifactDownloadPolicy::default();
    let watch_copy = async {
        loop {
            match tokio::fs::read_dir(directory).await {
                Ok(mut entries) => {
                    while let Some(entry) = entries.next_entry().await.unwrap() {
                        if entry.file_name().to_string_lossy().ends_with(".tmp")
                            && entry.metadata().await.unwrap().len() > 0
                        {
                            cancel.cancel();
                            return;
                        }
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => panic!("reading artifact cache: {error}"),
            }
            tokio::task::yield_now().await;
        }
    };
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        let (result, ()) = tokio::join!(
            materialize(
                std::slice::from_ref(&artifact),
                &cache,
                None,
                false,
                &policy,
                &cancel,
            ),
            watch_copy
        );
        result
    })
    .await
    .expect("observe and cancel the in-progress copy");
    assert!(matches!(result, Err(ArtifactError::Cancelled)));
    assert!(!destination.exists());
    assert!(
        tokio::fs::read_dir(directory)
            .await
            .unwrap()
            .next_entry()
            .await
            .unwrap()
            .is_none()
    );
    // Pending filesystem work must not recreate the cancelled temporary path.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        tokio::fs::read_dir(directory)
            .await
            .unwrap()
            .next_entry()
            .await
            .unwrap()
            .is_none()
    );
}
