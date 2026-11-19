# CONTRACTS.md — Cloud Attendance System

**Status:** Approved 
**Version:** 0.1.0  
**Owners:** All five project contributors  
**Rule:** No implementation, migrations, event producers/consumers, or Terraform resource creation until this document is approved by all contributors.

## 1. Scope and decision status

The course brief requires five microservices (authentication, students, facial recognition, attendance, notification), OpenStack DevStack (including Keystone and Swift), PostgreSQL, Kafka, and an attendance export using `P`/`A`. The exact names, fields, routes, and event schemas below are **proposed team contracts**, not prescribed by the brief. All times are UTC ISO-8601; IDs are UUID strings; JSON uses `snake_case`; REST routes are versioned `/api/v1`.

**Data minimization:** Facial identification is biometric processing. Use only authorized, consented or otherwise lawfully collected reference photos; restrict access and retention. Recognition results are provisional until validated. Never put image bytes, face embeddings, passwords, or tokens in Kafka events or logs.

## 2. Ownership and system boundaries

| Component | Owns | Depends on |
|---|---|---|
| `auth-service` | Application users, roles, Keystone integration | Keystone |
| `student-service` | Students, enrollments, official course roster, reference photo metadata | PostgreSQL, Swift |
| `facial-recognition-service` | Recognition jobs, face matches, model metadata | Kafka, Swift, Hugging Face model |
| `attendance-service` | Sessions, attendance records, approvals, exports | PostgreSQL, Kafka, student roster |
| `notification-service` | Notification requests and delivery status | Kafka / external notification provider |
| Frontend | UI only; no direct database or Swift admin access | REST APIs |

**Database rule:** One PostgreSQL deployment is acceptable for the mini-project, but use separate schemas (`auth`, `students`, `recognition`, `attendance`, `notifications`) and separate database roles. Only the owning service writes its schema; other services use APIs/events. No cross-schema foreign keys. IDs across schemas are logical references.

## 3. Kafka contracts

**Broker:** `kafka:9092` inside the deployment network (environment-configurable).  
**Serialization:** UTF-8 JSON, `Content-Type: application/json`.  
**Topic names:** `photo.uploaded`, `faces.recognized`.  
**Partition key:** `session_id` for both topics (preserves per-session ordering within a topic).  
**Delivery:** at least once; consumers MUST be idempotent using `event_id`, and MUST tolerate retries and out-of-order processing across topics.  
**Producer/consumer:** Attendance produces `photo.uploaded` after confirmed Swift upload and persisted job; Recognition consumes it and produces `faces.recognized`; Attendance consumes the latter.  
**Retry/error handling:** bounded retries with exponential backoff; then `photo.uploaded.dlq` or `faces.recognized.dlq` containing sanitized original event and error metadata, without images or secrets.  
**Versioning:** `schema_version` is required; breaking changes require a new major version and coordinated migration.

### 3.1 Event envelope (required for both)

| Field | Type | Rule |
|---|---|---|
| `event_id` | UUID | Unique for this event; idempotency key |
| `event_type` | string | Exact topic/event name |
| `schema_version` | string | `1.0` |
| `occurred_at` | string | UTC RFC 3339 timestamp |
| `correlation_id` | UUID | Trace one photo-to-attendance workflow |
| `session_id` | UUID | Attendance session |
| `payload` | object | Event-specific body |

### 3.2 `photo.uploaded` (v1.0)

```json
{
  "event_id": "6d40edcc-52c8-4ed8-8c76-ef9d0a884f51",
  "event_type": "photo.uploaded",
  "schema_version": "1.0",
  "occurred_at": "2026-11-18T17:00:00Z",
  "correlation_id": "7c47ad08-98c7-4a44-9e98-0c7113e3a03b",
  "session_id": "f79f57f4-bf2e-4ad1-85ad-1ab3fb183e7c",
  "payload": {
    "photo_id": "a58919c6-3c29-4554-9999-2f6c9aef12ca",
    "recognition_job_id": "d51b4e32-49d6-48c6-a6b7-bf34cbf3e2c1",
    "uploaded_by": "1cb72d91-8f8d-4a8a-9c54-7c7c3a32e110",
    "swift_container": "attendance-classroom-photos",
    "swift_object_key": "sessions/f79f57f4-bf2e-4ad1-85ad-1ab3fb183e7c/a58919c6-3c29-4554-9999-2f6c9aef12ca.jpg",
    "content_type": "image/jpeg",
    "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  }
}
```

Required payload fields are all shown. `sha256` is lowercase 64-character hexadecimal. Only supported MIME types: `image/jpeg`, `image/png`; extension must match MIME type. Consumer reads the image from Swift with service credentials, not a public URL.

### 3.3 `faces.recognized` (v1.0)

```json
{
  "event_id": "832af135-0b13-4c86-92a4-c9a4ad80d62a",
  "event_type": "faces.recognized",
  "schema_version": "1.0",
  "occurred_at": "2026-11-18T17:00:07Z",
  "correlation_id": "7c47ad08-98c7-4a44-9e98-0c7113e3a03b",
  "session_id": "f79f57f4-bf2e-4ad1-85ad-1ab3fb183e7c",
  "payload": {
    "photo_id": "a58919c6-3c29-4554-9999-2f6c9aef12ca",
    "recognition_job_id": "d51b4e32-49d6-48c6-a6b7-bf34cbf3e2c1",
    "status": "completed",
    "model_id": "approved-model-name",
    "model_version": "pinned-revision",
    "matches": [
      {
        "face_index": 0,
        "student_id": "abf73d37-9b60-431d-8a63-d7cf568c1f0b",
        "confidence": 0.94,
        "decision": "candidate"
      }
    ],
    "unmatched_face_count": 0,
    "error_code": null
  }
}
```

`status` is `completed` or `failed`; on failure, `matches=[]` and `error_code` is a nonempty safe string. `confidence` is a finite number from 0 to 1 (not automatically a calibrated probability). `decision` is `candidate` or `needs_review`; `student_id` may be null for ambiguous/unmatched faces. Each `face_index` is unique within a job. Duplicate student matches must be flagged for review, not counted twice. The model, threshold, and calibration policy require separate team approval. **No recognition result by itself finalizes attendance.**

## 4. PostgreSQL logical schema (proposed)

Use UUID primary keys, `created_at TIMESTAMPTZ NOT NULL DEFAULT now()`, and UTC storage. `NOT NULL` unless indicated as nullable (`?`). Unique constraints are explicit below. Foreign keys apply **only inside the same schema**.

| Schema.table | Columns (besides `created_at`) | Constraints / purpose |
|---|---|---|
| `auth.users` | `id UUID PK`, `keystone_subject TEXT`, `email TEXT`, `role TEXT`, `is_active BOOLEAN` | Unique `keystone_subject`, unique `email`; role `admin/teacher` |
| `students.students` | `id UUID PK`, `student_number TEXT`, `full_name TEXT`, `is_active BOOLEAN` | Unique `student_number` |
| `students.courses` | `id UUID PK`, `code TEXT`, `title TEXT` | Unique `code` |
| `students.enrollments` | `id UUID PK`, `course_id UUID FK`, `student_id UUID FK`, `status TEXT` | Unique `(course_id,student_id)`; official roster is active enrollments |
| `students.reference_photos` | `id UUID PK`, `student_id UUID FK`, `swift_container TEXT`, `swift_object_key TEXT`, `sha256 CHAR(64)`, `consent_recorded_at TIMESTAMPTZ?`, `deleted_at TIMESTAMPTZ?` | Unique `(swift_container,swift_object_key)`; no image bytes |
| `attendance.sessions` | `id UUID PK`, `course_id UUID`, `teacher_id UUID`, `starts_at TIMESTAMPTZ`, `status TEXT`, `roster_snapshot_at TIMESTAMPTZ?` | Status `draft/processing/review/approved/closed`; IDs reference other services logically |
| `attendance.session_roster` | `session_id UUID FK`, `student_id UUID`, `student_number TEXT`, `full_name TEXT` | Composite PK `(session_id,student_id)`; immutable official roster snapshot for that session |
| `attendance.photos` | `id UUID PK`, `session_id UUID FK`, `uploaded_by UUID`, `swift_container TEXT`, `swift_object_key TEXT`, `content_type TEXT`, `sha256 CHAR(64)` | Unique `(swift_container,swift_object_key)` |
| `recognition.jobs` | `id UUID PK`, `session_id UUID`, `photo_id UUID`, `status TEXT`, `model_id TEXT?`, `model_version TEXT?`, `error_code TEXT?`, `finished_at TIMESTAMPTZ?` | Unique `photo_id`; status `queued/processing/completed/failed` |
| `recognition.matches` | `id UUID PK`, `job_id UUID FK`, `face_index INT`, `student_id UUID?`, `confidence DOUBLE PRECISION?`, `decision TEXT` | Unique `(job_id,face_index)`; confidence 0..1 |
| `attendance.records` | `session_id UUID FK`, `student_id UUID`, `status CHAR(1)`, `source TEXT`, `reviewed_by UUID?`, `reviewed_at TIMESTAMPTZ?` | Composite PK `(session_id,student_id)`; status `P/A`; one row per roster student at approval |
| `attendance.audit_log` | `id UUID PK`, `session_id UUID`, `actor_id UUID?`, `action TEXT`, `details JSONB` | Append-only; no biometric data or secrets |
| `notifications.deliveries` | `id UUID PK`, `session_id UUID`, `recipient_id UUID`, `channel TEXT`, `status TEXT`, `sent_at TIMESTAMPTZ?` | Delivery tracking; no sensitive content in logs |
| `attendance.processed_events` | `event_id UUID PK`, `event_type TEXT`, `processed_at TIMESTAMPTZ` | Consumer idempotency |
| `recognition.processed_events` | `event_id UUID PK`, `event_type TEXT`, `processed_at TIMESTAMPTZ` | Consumer idempotency |

**Consistency rules:** The attendance service snapshots the official roster when processing starts; only roster members may receive P/A records. `A` means not confirmed present, not proof of absence. Recognition candidates require teacher review before approval. Missing/failed photos do not automatically finalize absences. Exports use the approved snapshot. Session closure freezes records; corrections require an audited reopening process (to be approved).

## 5. REST API contracts

**Base path:** `/api/v1`. HTTPS in deployed environments. JSON request/response except image uploads and CSV downloads. UUID path parameters. Authentication: Keystone-backed bearer token; service validates issuer/audience/expiry and role. Teachers can operate only on authorized courses; admins manage rosters. All endpoints require authentication except health probes.

| Owner | Method | Endpoint | Request / response | Role |
|---|---|---|---|---|
| Auth | `GET` | `/auth/me` | `200 {"user_id":"uuid","role":"teacher","email":"..."}` | Authenticated |
| Students | `POST` | `/students` | `{student_number,full_name}` → `201 {id,...}` | Admin |
| Students | `GET` | `/students/{student_id}` | `200 {id,student_number,full_name}` | Admin/authorized teacher |
| Students | `POST` | `/courses` | `{code,title}` → `201 {id,...}` | Admin |
| Students | `POST` | `/courses/{course_id}/enrollments` | `{student_id}` → `201` | Admin |
| Students | `GET` | `/courses/{course_id}/roster` | `200 {course_id,students:[{id,student_number,full_name}]}` | Admin/authorized teacher |
| Students | `POST` | `/students/{student_id}/reference-photos` | `multipart/form-data` `file` → `201 {photo_id,swift_container,swift_object_key}` | Admin; lawful basis required |
| Attendance | `POST` | `/attendance/sessions` | `{course_id,starts_at}` → `201 {id,status}` | Authorized teacher |
| Attendance | `POST` | `/attendance/sessions/{session_id}/photos` | `multipart/form-data` `file` → `202 {photo_id,recognition_job_id,status:"queued"}` | Authorized teacher |
| Attendance | `GET` | `/attendance/sessions/{session_id}` | `200 {id,course_id,status,starts_at}` | Authorized teacher/admin |
| Recognition | `GET` | `/recognition/jobs/{job_id}` | `200 {id,status,model_id,model_version,matches:[...]}` | Authorized teacher/admin |
| Attendance | `GET` | `/attendance/sessions/{session_id}/review` | `200 {roster:[{student_id,student_number,full_name,suggested_status,needs_review}]}` | Authorized teacher |
| Attendance | `PUT` | `/attendance/sessions/{session_id}/records/{student_id}` | `{status:"P"|"A",reason:"..."}` → `200 {student_id,status}` | Authorized teacher; audited |
| Attendance | `POST` | `/attendance/sessions/{session_id}/approve` | `{}` → `200 {id,status:"approved"}` | Authorized teacher |
| Attendance | `GET` | `/attendance/sessions/{session_id}/export` | `200 text/csv` columns `student_number,full_name,status` | Authorized teacher/admin; approved only |
| Notifications | `GET` | `/notifications/{delivery_id}` | `200 {id,status,channel}` | Authorized recipient/admin |

**API behavior:** Validate uploads (JPEG/PNG, max 10 MiB proposed), reject unsupported MIME/oversize. Return standardized errors:

```json
{
  "error": {
    "code": "INVALID_STATE",
    "message": "Session must be in review before approval",
    "correlation_id": "7c47ad08-98c7-4a44-9e98-0c7113e3a03b"
  }
}
```

Status codes: `400` invalid input, `401` unauthenticated, `403` forbidden, `404` unknown resource, `409` state/conflict, `413` oversized image, `415` unsupported format, `422` invalid semantic content, `500` internal error, `503` dependency unavailable. `202` means queued, **not** recognized or approved. List endpoints added later must define pagination before implementation. Requests that mutate state should accept `Idempotency-Key` (UUID) for safe retries; the backend must persist deduplication outcomes.

## 6. OpenStack Swift contract

**Account:** OpenStack project/service account provisioned via Keystone; account identifier comes from the environment (never hardcode tenant IDs). Swift names below are exact proposed resource names.

| Swift container | Object key pattern | Content | Access |
|---|---|---|---|
| `attendance-reference-photos` | `students/{student_id}/{photo_id}.{jpg|png}` | Student reference image | Student service write; recognition service read |
| `attendance-classroom-photos` | `sessions/{session_id}/{photo_id}.{jpg|png}` | Classroom upload | Attendance service write; recognition service read |
| `attendance-exports` | `sessions/{session_id}/exports/{export_id}.csv` | Approved P/A export (optional persisted copy) | Attendance service only; authorized short-lived retrieval |

**Rules:** Containers private by default; no public ACLs. Configure least-privilege Keystone/service credentials and Swift ACLs, encryption at rest if supported/configured, TLS in transit, and retention/deletion policies approved before use. Never store photos or embeddings in PostgreSQL; store only Swift object identifiers, hashes, and metadata. No signed/public image URLs in Kafka. Object keys are immutable per upload; retries must not silently overwrite objects. Terraform manages container configuration, not individual student photos. Do not commit `clouds.yaml`, `terraform.tfstate`, `*.tfvars` with credentials, or secrets.

**Retention proposal (requires privacy review):** classroom images 7 days after session approval; reference images until enrollment ends or lawful deletion request; exports 30 days. Actual deletion mechanisms and Swift capability support must be verified in DevStack. Do not claim retention/encryption is active until tested.

## 7. End-to-end workflow and failure rules

1. Teacher creates session; attendance service retrieves and snapshots the official course roster.
2. Teacher uploads a photo to Attendance API. Service validates image, uploads to private Swift, persists photo/job metadata, and publishes `photo.uploaded` reliably (transactional outbox or equivalent retryable publication).
3. Recognition consumer retrieves the image from Swift, runs the approved model against authorized reference images, stores job/match metadata, and publishes `faces.recognized` reliably.
4. Attendance consumer correlates `session_id`, `photo_id`, `recognition_job_id`, deduplicates events, and builds **provisional** matches against the snapshot roster.
5. Teacher reviews candidates and unresolved faces; manually confirms P/A for each roster student, with an audit trail.
6. Approval requires a complete roster with one validated P/A record per student; CSV export uses only approved records.
7. Notification service sends only authorized, minimal notifications. Notification trigger/event contract is **not yet specified** and must be agreed before notification integration.

On recognition failure, keep the session in review/error state, allow a retry or manual attendance, and never mark the whole class absent automatically. On Swift/Kafka/PostgreSQL outage, return a truthful pending/error state and retry safely without duplicate records.

## 8. Contract tests and change management

- Validate Kafka messages against committed JSON Schema files before publishing and consuming.
- Test successful, duplicate, delayed, invalid-version, failed, and DLQ events.
- Validate REST requests/responses using a committed OpenAPI 3.1 specification.
- Validate PostgreSQL migrations and ownership boundaries against a clean database.
- Verify Swift container names, ACLs, access denial, upload/retrieval, and deletion in DevStack.
- Test roster snapshot, unknown face, duplicate match, manual correction, failed recognition, and final CSV P/A export.
- Breaking contract changes require a PR, impact review by affected owners, version bump, and approval from all five contributors.

**Planned source-of-truth files after approval:** `contracts/events/photo.uploaded.schema.json`, `contracts/events/faces.recognized.schema.json`, `contracts/openapi.yaml`, `database/migrations/`, `infrastructure/terraform/`. This document is the agreement; machine-readable schemas must match it exactly.

## 9. Decisions requiring explicit team agreement

- [ ] Confirm service ownership and schema isolation.
- [ ] Confirm exact Kafka topic names, payloads, retry/DLQ policy, and event versioning.
- [ ] Confirm PostgreSQL columns, constraints, and roster snapshot semantics.
- [ ] Confirm REST endpoints, authorization model, and upload size limit.
- [ ] Confirm Swift container names, ACLs, retention, and encryption feasibility in DevStack.
- [ ] Select and pin the Hugging Face model; define confidence threshold and human-review policy.
- [ ] Define biometric data lawful basis, access control, deletion, and retention policy.
- [ ] Define notification triggering contract or defer notification implementation.
- [ ] Approve failure behavior, contract tests, and migration/change policy.

## 10. Sign-off — required before coding

| Contributor | Assigned ownership | Approval (`APPROVED` / `CHANGES REQUESTED`) | Date |
|---|---|---|---|
| Member 1 | Auth | PENDING | — |
| Member 2 | Students | PENDING | — |
| Member 3 | Recognition | PENDING | — |
| Member 4 | Attendance | PENDING | — |
| Member 5 | Notifications / infrastructure | PENDING | — |

**Approval gate:** Change document status to `APPROVED — v1.0.0` only after all five members approve the same Git commit and open decisions are resolved. Record that commit hash here. Until then, this is a proposal, not an implementation authorization.

**Approved commit:** `PENDING`
