// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The cache HTTP API: `/api/mounts`, `/api/ufs/list` and the load/export job
//! routes ClusterBoard and the preheat UI read. Upstream serves jobs from the
//! transfer service; these handlers keep the master-side paths the UI already
//! uses and proxy to whoever owns the jobs (embedded service or standalone
//! transfer deployment).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Extension, Path, Query};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use curvine_data_transfer::transfer::EmbeddedTransfer;
use curvine_error::ErrorKind;
use curvine_fs_api::{FileSystem, Path as UfsPath};
use curvine_model::{
    MountInfo, TransferCommand, TransferJobRecord, TransferKind, TransferState,
};
use curvine_proto::{
    GetTransferStatusResponse, ListTransfersRequest, SubmitTransferRequest, TransferJobStatusProto,
    TransferKindProto, TransferProgressProto, TransferStateProto,
};
use curvine_transfer_client::TransferClient;
use curvine_unified_fs::UfsFileSystem;

use crate::master::MountManager;

/// One `/api/ufs/list` answer holds at most this many entries; `truncated`
/// says the directory had more. A UI tree needs a page, not a million keys.
const UFS_LIST_MAX_ENTRIES: usize = 5000;

/// What the cache handlers need from the master. Kept in its own struct so the
/// router handler does not grow a field per dependency.
#[derive(Clone)]
pub(crate) struct CacheApiState {
    pub(crate) mount_manager: Arc<MountManager>,
    /// The transfer service embedded in this master, when the deployment runs
    /// it in-process.
    pub(crate) transfer: Option<Arc<EmbeddedTransfer>>,
    /// RPC client for a standalone transfer deployment. The HTTP surface is the
    /// same in both modes.
    pub(crate) transfer_client: Option<TransferClient>,
}

/// The cache API routes, with their own extension state.
pub(crate) fn router(state: CacheApiState) -> Router {
    Router::new()
        .route("/api/mounts", get(mounts))
        .route("/api/ufs/list", get(ufs_list))
        .route("/api/transfers", get(transfers).post(submit_transfer))
        .route("/api/load/jobs", get(load_jobs).post(submit_load_job))
        .route("/api/load/jobs/:job_id", get(load_job_detail))
        .route("/api/load/jobs/:job_id/cancel", post(cancel_load_job))
        .route("/api/export/jobs", get(export_jobs))
        .route("/api/export/jobs/:job_id", get(export_job_detail))
        .route("/api/export/jobs/:job_id/cancel", post(cancel_export_job))
        .layer(Extension(state))
}

const TRANSFER_LIST_DEFAULT_LIMIT: u32 = 20;
/// Upper bound of `/api/transfers?limit=`, matching the RPC page cap.
const TRANSFER_LIST_MAX_LIMIT: u32 = 1000;

fn transfer_kind_name(kind: TransferKind) -> &'static str {
    match kind {
        TransferKind::Load => "load",
        TransferKind::Export => "export",
    }
}

fn transfer_state_name(state: TransferState) -> &'static str {
    match state {
        TransferState::Pending => "pending",
        TransferState::Planning => "planning",
        TransferState::Dispatching => "dispatching",
        TransferState::Running => "running",
        TransferState::Canceling => "canceling",
        TransferState::Completed => "completed",
        TransferState::Failed => "failed",
        TransferState::Canceled => "canceled",
        TransferState::PartialSuccess => "partial-success",
    }
}

fn parse_transfer_kind(raw: &str) -> Option<TransferKind> {
    match raw.to_ascii_lowercase().as_str() {
        "load" => Some(TransferKind::Load),
        "export" => Some(TransferKind::Export),
        _ => None,
    }
}

fn parse_transfer_state(raw: &str) -> Option<TransferState> {
    match raw.to_ascii_lowercase().replace('_', "-").as_str() {
        "pending" => Some(TransferState::Pending),
        "planning" => Some(TransferState::Planning),
        "dispatching" => Some(TransferState::Dispatching),
        "running" => Some(TransferState::Running),
        "canceling" => Some(TransferState::Canceling),
        "completed" => Some(TransferState::Completed),
        "failed" => Some(TransferState::Failed),
        "canceled" => Some(TransferState::Canceled),
        "partial-success" => Some(TransferState::PartialSuccess),
        _ => None,
    }
}

/// One job, without the planner inputs (`command_json`, mount and secret
/// snapshots) that `TransferJobRecord` also carries.
fn transfer_job_json(job: &TransferJobRecord) -> Value {
    json!({
        "job_id": job.job_id,
        "run_id": job.run_id,
        "kind": transfer_kind_name(job.kind),
        "state": transfer_state_name(job.state),
        "source_path": job.source_path,
        "target_path": job.target_path,
        "submitter": job.submitter,
        "tenant": job.tenant,
        "owner": job.owner,
        "progress": {
            "loaded_size": job.summary.loaded_size,
            "total_size": job.summary.total_size,
            "total_files": job.summary.total_files,
            "loaded_files": job.summary.loaded_files,
            "skipped_files": job.summary.skipped_files,
            "skipped_size": job.summary.skipped_size,
            "update_time": job.summary.update_time,
            "message": job.summary.message,
        },
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    })
}

fn json_error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "message": message.into() }))).into_response()
}

fn proto_kind_name(code: i32) -> &'static str {
    match TransferKindProto::from_i32(code) {
        Some(TransferKindProto::TransferLoad) => "load",
        Some(TransferKindProto::TransferExport) => "export",
        None => "unknown",
    }
}

fn proto_state_name(code: i32) -> &'static str {
    match TransferStateProto::from_i32(code) {
        Some(TransferStateProto::TransferPending) => "pending",
        Some(TransferStateProto::TransferPlanning) => "planning",
        Some(TransferStateProto::TransferDispatching) => "dispatching",
        Some(TransferStateProto::TransferRunning) => "running",
        Some(TransferStateProto::TransferCanceling) => "canceling",
        Some(TransferStateProto::TransferCompleted) => "completed",
        Some(TransferStateProto::TransferFailed) => "failed",
        Some(TransferStateProto::TransferCanceled) => "canceled",
        Some(TransferStateProto::TransferPartialSuccess) => "partial-success",
        None => "unknown",
    }
}

fn transfer_proto_progress_json(progress: &TransferProgressProto) -> Value {
    json!({
        "loaded_size": progress.loaded_size,
        "total_size": progress.total_size,
        "total_files": progress.total_files.unwrap_or_default(),
        "loaded_files": progress.loaded_files.unwrap_or_default(),
        "skipped_files": progress.skipped_files.unwrap_or_default(),
        "skipped_size": progress.skipped_size.unwrap_or_default(),
        "update_time": progress.update_time,
        "message": progress.message,
    })
}

fn transfer_proto_job_json(job: &TransferJobStatusProto) -> Value {
    json!({
        "job_id": job.job_id,
        "run_id": job.run_id,
        "kind": proto_kind_name(job.kind),
        "state": proto_state_name(job.state),
        "source_path": job.source_path,
        "target_path": job.target_path,
        "submitter": job.submitter,
        "tenant": job.tenant,
        "owner": job.owner.clone().unwrap_or_default(),
        "progress": transfer_proto_progress_json(&job.progress),
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    })
}

fn transfer_proto_status_json(detail: &GetTransferStatusResponse) -> Value {
    json!({
        "job_id": detail.job_id,
        "run_id": detail.run_id,
        "kind": detail.kind.map(proto_kind_name).unwrap_or("unknown"),
        "state": proto_state_name(detail.state),
        "source_path": detail.source_path.clone().unwrap_or_default(),
        "target_path": detail.target_path.clone().unwrap_or_default(),
        "submitter": detail.submitter.clone().unwrap_or_default(),
        "tenant": detail.tenant.clone().unwrap_or_default(),
        "owner": detail.owner.clone().unwrap_or_default(),
        "progress": transfer_proto_progress_json(&detail.progress),
        "created_at": detail.created_at.unwrap_or_default(),
        "updated_at": detail.updated_at.unwrap_or_default(),
    })
}

#[derive(Deserialize)]
struct SubmitTransferBody {
    source_path: String,
    target_path: String,
    #[serde(default)]
    files: Vec<String>,
    #[serde(default)]
    overwrite: Option<bool>,
    #[serde(default)]
    submitter: Option<String>,
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    force: bool,
}

fn normalized_file_list(files: Vec<String>) -> Vec<String> {
    let mut out = Vec::with_capacity(files.len());
    for file in files {
        let file = file.trim();
        if file.is_empty() || file.starts_with('#') {
            continue;
        }
        out.push(file.to_string());
    }
    out
}

/// `POST /api/transfers` — submit a load. JSON body:
/// `{source_path, target_path, files?: [...], overwrite?, submitter?, tenant?, force?}`.
/// A `text/plain` body is treated as the file list, with `source_path` and
/// `target_path` given as query parameters.
async fn submit_transfer(
    Extension(state): Extension<CacheApiState>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let body = if content_type.starts_with("application/json") {
        match serde_json::from_slice::<SubmitTransferBody>(&body) {
            Ok(body) => body,
            Err(err) => {
                return json_error(StatusCode::BAD_REQUEST, format!("invalid JSON body: {err}"))
            }
        }
    } else {
        let source_path = match params.get("source_path") {
            Some(path) if !path.trim().is_empty() => path.clone(),
            _ => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "source_path query parameter is required for a text file list",
                )
            }
        };
        let target_path = match params.get("target_path") {
            Some(path) if !path.trim().is_empty() => path.clone(),
            _ => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "target_path query parameter is required for a text file list",
                )
            }
        };
        let text = match std::str::from_utf8(&body) {
            Ok(text) => text,
            Err(_) => return json_error(StatusCode::BAD_REQUEST, "file list must be UTF-8 text"),
        };
        SubmitTransferBody {
            source_path,
            target_path,
            files: normalized_file_list(text.lines().map(str::to_string).collect()),
            overwrite: params.get("overwrite").map(|value| value != "false"),
            submitter: params.get("submitter").cloned(),
            tenant: params.get("tenant").cloned(),
            force: params.get("force").is_some_and(|value| value != "false"),
        }
    };

    if body.source_path.trim().is_empty() || body.target_path.trim().is_empty() {
        return json_error(
            StatusCode::BAD_REQUEST,
            "source_path and target_path are required",
        );
    }
    let files = normalized_file_list(body.files);
    let overwrite = body.overwrite.unwrap_or(!files.is_empty());
    let submitter = body
        .submitter
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "http-api".to_string());
    let tenant = body.tenant.unwrap_or_default();
    let mut command = TransferCommand {
        kind: TransferKind::Load,
        source_path: body.source_path.clone(),
        target_path: body.target_path.clone(),
        client_request_id: TransferCommand::default_client_request_id_with_overwrite(
            TransferKind::Load,
            &body.source_path,
            &body.target_path,
            overwrite,
        ),
        submitter: submitter.clone(),
        tenant: tenant.clone(),
        options: BTreeMap::new(),
        files: files.clone(),
    };
    if let Some(digest) = command.files_digest() {
        command.client_request_id = format!("{}_files_{digest}", command.client_request_id);
    }
    command.set_overwrite(overwrite);

    if let Some(transfer) = state.transfer.as_ref() {
        let command_bytes = match serde_json::to_vec(&command) {
            Ok(bytes) => bytes,
            Err(err) => {
                return json_error(StatusCode::BAD_REQUEST, format!("invalid command: {err}"))
            }
        };
        let req = SubmitTransferRequest {
            kind: TransferKind::Load as i32,
            source_path: command.source_path.clone(),
            target_path: command.target_path.clone(),
            client_request_id: command.client_request_id.clone(),
            submitter: command.submitter.clone(),
            tenant: command.tenant.clone(),
            command: command_bytes,
            protocol_version: Some(1),
        };
        return match transfer.service().submit_transfer(req) {
            Ok(job) => {
                let mut job = job;
                if body.force && job.state.is_terminal() {
                    match transfer.service().retry_transfer(&job.job_id) {
                        Ok(retried) => job = retried,
                        Err(err) => {
                            return json_error(StatusCode::BAD_REQUEST, err.to_string());
                        }
                    }
                }
                (
                    StatusCode::ACCEPTED,
                    Json(json!({
                        "job_id": job.job_id,
                        "run_id": job.run_id,
                        "state": transfer_state_name(job.state),
                        "files": files.len(),
                    })),
                )
                    .into_response()
            }
            Err(err) => json_error(StatusCode::BAD_REQUEST, err.to_string()),
        };
    }

    let Some(client) = state.transfer_client.as_ref() else {
        return json_error(StatusCode::BAD_GATEWAY, "transfer service is unavailable");
    };
    match client.submit(command).await {
        Ok(mut resp) => {
            if body.force && TransferState::from(resp.state).is_terminal() {
                match client.retry(&resp.job_id).await {
                    Ok(retried) => resp = retried,
                    Err(err) => {
                        log::error!("retrying transfer submit failed: {err}");
                        return json_error(
                            StatusCode::BAD_GATEWAY,
                            "transfer service is unavailable",
                        );
                    }
                }
            }
            (
                StatusCode::ACCEPTED,
                Json(json!({
                    "job_id": resp.job_id,
                    "run_id": resp.run_id,
                    "state": transfer_state_name(TransferState::from(resp.state)),
                    "files": files.len(),
                })),
            )
                .into_response()
        }
        Err(err) => {
            log::error!("proxying transfer submit failed: {err}");
            json_error(StatusCode::BAD_GATEWAY, "transfer service is unavailable")
        }
    }
}

/// `GET /api/transfers` — one page of transfer jobs, newest first.
///
/// Read-only, like the rest of `/api/*`. In the default embedded mode the jobs
/// live in this master's memory, so only the Active master has them and only the
/// last `transfer.terminal_retention` (10 min by default) of finished jobs
/// survive.
async fn transfers(
    Extension(state): Extension<CacheApiState>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let kind = match params.get("kind") {
        Some(raw) => match parse_transfer_kind(raw) {
            Some(kind) => Some(kind),
            None => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    format!("unknown kind '{raw}'; expected load or export"),
                )
            }
        },
        None => None,
    };
    let state_filter = match params.get("state") {
        Some(raw) => match parse_transfer_state(raw) {
            Some(state) => Some(state),
            None => return json_error(StatusCode::BAD_REQUEST, format!("unknown state '{raw}'")),
        },
        None => None,
    };
    let page_size = match params.get("limit") {
        Some(raw) => match raw.parse::<u32>() {
            Ok(limit) if limit > 0 => Some(limit.min(TRANSFER_LIST_MAX_LIMIT)),
            _ => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    format!("invalid limit '{raw}'; expected a positive integer"),
                )
            }
        },
        None => Some(TRANSFER_LIST_DEFAULT_LIMIT),
    };
    let page_token = params.get("page_token").cloned();

    if let Some(transfer) = state.transfer.as_ref() {
        let req = ListTransfersRequest {
            kind: kind.map(|kind| kind as i32),
            state: state_filter.map(|state| state as i32),
            page_size,
            page_token,
            submitter: params.get("submitter").cloned(),
            tenant: params.get("tenant").cloned(),
        };
        return match transfer.service().list_transfer_records(req) {
            Ok((jobs, next_page_token)) => Json(json!({
                "jobs": jobs.iter().map(transfer_job_json).collect::<Vec<_>>(),
                "next_page_token": next_page_token,
            }))
            .into_response(),
            Err(err) => err.into_response(),
        };
    }

    let Some(client) = state.transfer_client.as_ref() else {
        return json_error(StatusCode::BAD_GATEWAY, "transfer service is unavailable");
    };
    match client
        .list(
            kind,
            state_filter,
            params.get("submitter").cloned(),
            params.get("tenant").cloned(),
            page_size,
            page_token,
        )
        .await
    {
        Ok(resp) => Json(json!({
            "jobs": resp.jobs.iter().map(transfer_proto_job_json).collect::<Vec<_>>(),
            "next_page_token": resp.next_page_token,
        }))
        .into_response(),
        Err(err) => {
            log::error!("proxying transfer list failed: {err}");
            json_error(StatusCode::BAD_GATEWAY, "transfer service is unavailable")
        }
    }
}

/// `POST /api/load/jobs` — submit a load; the transfer kind is fixed to load.
async fn submit_load_job(
    Extension(state): Extension<CacheApiState>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    submit_transfer(Extension(state), Query(params), headers, body).await
}

/// `GET /api/load/jobs/{job_id}`.
async fn load_job_detail(
    Extension(state): Extension<CacheApiState>,
    Path(job_id): Path<String>,
) -> Response {
    transfer_detail_for(state, job_id, Some(TransferKind::Load)).await
}

async fn transfer_detail_for(
    state: CacheApiState,
    job_id: String,
    expected_kind: Option<TransferKind>,
) -> Response {
    let mismatch = || json_error(StatusCode::NOT_FOUND, format!("job {job_id} not found"));

    if let Some(transfer) = state.transfer.as_ref() {
        return match transfer.service().get_transfer(&job_id) {
            Ok(job) if expected_kind.is_none_or(|kind| job.kind == kind) => {
                Json(transfer_job_json(&job)).into_response()
            }
            Ok(_) => mismatch(),
            Err(err) if matches!(err.kind(), ErrorKind::JobNotFound) => {
                json_error(StatusCode::NOT_FOUND, err.to_string())
            }
            Err(err) => err.into_response(),
        };
    }

    let Some(client) = state.transfer_client.as_ref() else {
        return json_error(StatusCode::BAD_GATEWAY, "transfer service is unavailable");
    };
    match client.status_summary(&job_id).await {
        Ok(detail) if expected_kind.is_none_or(|kind| detail.kind == Some(kind as i32)) => {
            Json(transfer_proto_status_json(&detail)).into_response()
        }
        Ok(_) => mismatch(),
        Err(err) if matches!(err.kind(), ErrorKind::JobNotFound) => {
            json_error(StatusCode::NOT_FOUND, err.to_string())
        }
        Err(err) => {
            log::error!("proxying transfer detail failed: {err}");
            json_error(StatusCode::BAD_GATEWAY, "transfer service is unavailable")
        }
    }
}

/// `GET /api/load/jobs` — load jobs only; the transfer kind is fixed so the
/// public cache API never exposes export/job internals as a filter.
async fn load_jobs(
    Extension(state): Extension<CacheApiState>,
    Query(mut params): Query<HashMap<String, String>>,
) -> Response {
    params.insert("kind".to_string(), "load".to_string());
    transfers(Extension(state), Query(params)).await
}

async fn export_job_detail(
    Extension(state): Extension<CacheApiState>,
    Path(job_id): Path<String>,
) -> Response {
    transfer_detail_for(state, job_id, Some(TransferKind::Export)).await
}

/// `GET /api/export/jobs` — export jobs only.
async fn export_jobs(
    Extension(state): Extension<CacheApiState>,
    Query(mut params): Query<HashMap<String, String>>,
) -> Response {
    params.insert("kind".to_string(), "export".to_string());
    transfers(Extension(state), Query(params)).await
}

/// `POST /api/load/jobs/{job_id}/cancel`.
async fn cancel_load_job(
    Extension(state): Extension<CacheApiState>,
    Path(job_id): Path<String>,
) -> Response {
    cancel_job_for(state, job_id, TransferKind::Load).await
}

/// `POST /api/export/jobs/{job_id}/cancel`.
async fn cancel_export_job(
    Extension(state): Extension<CacheApiState>,
    Path(job_id): Path<String>,
) -> Response {
    cancel_job_for(state, job_id, TransferKind::Export).await
}

async fn cancel_job_for(
    state: CacheApiState,
    job_id: String,
    expected_kind: TransferKind,
) -> Response {
    let mismatch = || json_error(StatusCode::NOT_FOUND, format!("job {job_id} not found"));
    if let Some(transfer) = state.transfer.as_ref() {
        let job = match transfer.service().get_transfer(&job_id) {
            Ok(job) if job.kind == expected_kind => job,
            Ok(_) => return mismatch(),
            Err(err) if matches!(err.kind(), ErrorKind::JobNotFound) => {
                return json_error(StatusCode::NOT_FOUND, err.to_string())
            }
            Err(err) => return err.into_response(),
        };
        return match transfer.service().request_cancel(&job.job_id, None) {
            Ok(new_state) => Json(json!({
                "job_id": job.job_id,
                "state": transfer_state_name(new_state),
            }))
            .into_response(),
            Err(err) => json_error(StatusCode::BAD_REQUEST, err.to_string()),
        };
    }

    let Some(client) = state.transfer_client.as_ref() else {
        return json_error(StatusCode::BAD_GATEWAY, "transfer service is unavailable");
    };
    match client.status_summary(&job_id).await {
        Ok(detail) if detail.kind == Some(expected_kind as i32) => {}
        Ok(_) => return mismatch(),
        Err(err) if matches!(err.kind(), ErrorKind::JobNotFound) => {
            return json_error(StatusCode::NOT_FOUND, err.to_string())
        }
        Err(err) => {
            log::error!("proxying transfer kind check failed: {err}");
            return json_error(StatusCode::BAD_GATEWAY, "transfer service is unavailable");
        }
    }
    match client.cancel(&job_id, None).await {
        Ok(resp) => Json(json!({
            "job_id": resp.job_id,
            "state": transfer_state_name(TransferState::from(resp.state)),
        }))
        .into_response(),
        Err(err) => {
            log::error!("proxying transfer cancel failed: {err}");
            json_error(StatusCode::BAD_GATEWAY, "transfer service is unavailable")
        }
    }
}

/// `GET /api/mounts` — the UFS mount table. `properties` holds each mount's
/// UFS credentials, so a mount is returned as the fields a UI needs and the
/// secrets stay on the master.
async fn mounts(Extension(state): Extension<CacheApiState>) -> Response {
    match state.mount_manager.get_mount_table() {
        Ok(table) => {
            let mut items: Vec<Value> = table.iter().map(mount_json).collect();
            items.sort_by(|a, b| json_str(a, "cv_path").cmp(json_str(b, "cv_path")));
            Json(Value::Array(items)).into_response()
        }
        Err(err) => err.into_response(),
    }
}

fn json_str<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or_default()
}

fn mount_json(mount: &MountInfo) -> Value {
    json!({
        "mount_id": mount.mount_id,
        "cv_path": mount.cv_path,
        "ufs_path": mount.ufs_path,
        "write_type": mount.write_type,
        "access_mode": mount.access_mode,
        "provider": mount.provider,
        "auto_cache": mount.auto_cache,
        "read_verify_ufs": mount.read_verify_ufs,
        "ttl_ms": mount.ttl_ms,
    })
}

/// The mount that owns `path`: the longest `ufs_path` that is the path itself
/// or one of its parent directories. Mounts nest (`s3://b/x` can sit under
/// `s3://b`), so the longest wins.
fn mount_for_path<'a>(table: &'a [MountInfo], path: &str) -> Option<&'a MountInfo> {
    table
        .iter()
        .filter(|mount| path_covers(&mount.ufs_path, path))
        .max_by_key(|mount| mount.ufs_path.trim_end_matches('/').len())
}

fn path_covers(root: &str, path: &str) -> bool {
    let root = root.trim_end_matches('/');
    path == root
        || path
            .strip_prefix(root)
            .is_some_and(|rest| rest.starts_with('/'))
}

/// `mount.cv_path` joined with the part of `path` below the mount's UFS root.
fn cv_path_for(mount: &MountInfo, path: &str) -> String {
    let rest = path
        .strip_prefix(mount.ufs_path.trim_end_matches('/'))
        .unwrap_or_default()
        .trim_matches('/');
    let cv = mount.cv_path.trim_end_matches('/');
    if rest.is_empty() {
        cv.to_string()
    } else {
        format!("{cv}/{rest}")
    }
}

/// One level up, staying inside the mount; `None` at the mount's own root.
fn ufs_parent(mount: &MountInfo, path: &str) -> Option<String> {
    let root = mount.ufs_path.trim_end_matches('/');
    if path == root {
        return None;
    }
    match path.rfind('/') {
        Some(idx) if idx >= root.len() => Some(path[..idx].to_string()),
        _ => Some(root.to_string()),
    }
}

/// `GET /api/ufs/list?path=<ufs path>` — one directory level of the UFS behind
/// a mount, so a UI can offer the real dataset tree instead of the Curvine
/// namespace, which only holds the directories something has already cached.
/// Entries carry the Curvine path a preheat of them would land on.
async fn ufs_list(
    Extension(state): Extension<CacheApiState>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let raw = params
        .get("path")
        .map(|path| path.trim().to_string())
        .unwrap_or_default();
    if raw.is_empty() {
        return json_error(StatusCode::BAD_REQUEST, "path is required");
    }
    let path = raw.trim_end_matches('/').to_string();

    let table = match state.mount_manager.get_mount_table() {
        Ok(table) => table,
        Err(err) => return err.into_response(),
    };
    let Some(mount) = mount_for_path(&table, &path) else {
        return json_error(StatusCode::NOT_FOUND, format!("no mount covers {path}"));
    };

    let ufs = match UfsPath::new(mount.ufs_path.clone())
        .map_err(|err| format!("invalid mount path {}: {err}", mount.ufs_path))
        .and_then(|root| {
            UfsFileSystem::new(&root, mount.properties.clone(), mount.provider)
                .map_err(|err| format!("UFS unavailable for {}: {err}", mount.ufs_path))
        }) {
        Ok(ufs) => ufs,
        Err(message) => return json_error(StatusCode::BAD_GATEWAY, message),
    };

    let listed = match UfsPath::new(path.clone()) {
        Ok(listed) => match ufs.list_status(&listed).await {
            Ok(statuses) => statuses,
            Err(err) => {
                return json_error(
                    StatusCode::BAD_GATEWAY,
                    format!("listing {path} failed: {err}"),
                )
            }
        },
        Err(err) => {
            return json_error(
                StatusCode::BAD_REQUEST,
                format!("invalid path {path}: {err}"),
            )
        }
    };

    let total = listed.len();
    let mut entries: Vec<(bool, String, String, i64, i64)> = listed
        .iter()
        .map(|status| {
            let entry_path = format!("{path}/{}", status.name);
            (
                status.is_dir,
                status.name.to_lowercase(),
                entry_path,
                status.len,
                status.mtime,
            )
        })
        .collect();
    // Directories first, then by name: the order the tree UI renders.
    entries.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));

    let truncated = entries.len() > UFS_LIST_MAX_ENTRIES;
    let entries: Vec<Value> = entries
        .into_iter()
        .take(UFS_LIST_MAX_ENTRIES)
        .map(|(is_dir, _, entry_path, len, mtime)| {
            json!({
                "name": entry_path.rsplit('/').next().unwrap_or_default(),
                "path": entry_path,
                "cv_path": cv_path_for(mount, &entry_path),
                "is_dir": is_dir,
                "len": len,
                "mtime": mtime,
            })
        })
        .collect();

    Json(json!({
        "path": path,
        "parent": ufs_parent(mount, &path),
        "cv_path": cv_path_for(mount, &path),
        "mount": mount_json(mount),
        "total": total,
        "truncated": truncated,
        "entries": entries,
    }))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mount(cv_path: &str, ufs_path: &str) -> MountInfo {
        MountInfo {
            cv_path: cv_path.to_string(),
            ufs_path: ufs_path.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn mounts_hide_their_credentials() {
        let mut info = mount("/data/s3/meshy-datasets", "s3://meshy-datasets");
        info.properties
            .insert("s3.secret_key".to_string(), "secret".to_string());

        let value = mount_json(&info);

        assert!(value.get("properties").is_none());
        assert_eq!(value["ufs_path"], "s3://meshy-datasets");
        assert_eq!(value["cv_path"], "/data/s3/meshy-datasets");
    }

    #[test]
    fn the_longest_mount_covers_a_path() {
        let table = vec![
            mount("/data/s3/meshy-datasets", "s3://meshy-datasets"),
            mount("/data/s3/meshy-datasets-sub", "s3://meshy-datasets/sub"),
            mount("/data/oss/meshy-learning", "oss://meshy-learning"),
        ];

        assert_eq!(
            mount_for_path(&table, "s3://meshy-datasets")
                .unwrap()
                .cv_path,
            "/data/s3/meshy-datasets"
        );
        assert_eq!(
            mount_for_path(&table, "s3://meshy-datasets/sub/dir")
                .unwrap()
                .cv_path,
            "/data/s3/meshy-datasets-sub"
        );
        // A sibling whose name only shares a prefix is not covered.
        assert!(mount_for_path(&table, "s3://meshy-datasetsX").is_none());
        assert!(mount_for_path(&table, "s3://nope").is_none());
    }

    #[test]
    fn ufs_paths_map_to_curvine_paths_and_stop_at_the_mount_root() {
        let info = mount("/data/s3/datasets", "s3://meshy-datasets/");

        assert_eq!(
            cv_path_for(&info, "s3://meshy-datasets"),
            "/data/s3/datasets"
        );
        assert_eq!(
            cv_path_for(&info, "s3://meshy-datasets/a/b"),
            "/data/s3/datasets/a/b"
        );
        assert_eq!(ufs_parent(&info, "s3://meshy-datasets"), None);
        assert_eq!(
            ufs_parent(&info, "s3://meshy-datasets/a").as_deref(),
            Some("s3://meshy-datasets")
        );
        assert_eq!(
            ufs_parent(&info, "s3://meshy-datasets/a/b").as_deref(),
            Some("s3://meshy-datasets/a")
        );
    }
}
