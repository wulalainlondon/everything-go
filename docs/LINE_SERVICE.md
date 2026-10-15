# LINE account service

This implementation adds a Go-owned LINE control plane and one isolated Node
account process per LINE identity. The initial host is macOS. It uses the pinned
unofficial LINEJS adapter; capabilities that still need a phone test are reported
separately from implemented code and live read-only checks.

## Source and ownership

* `internal/linebridge`: account grants, encrypted operation/resource storage,
  fixed sender bindings, schedules, receipts, call reservations and supervision.
* `services/line-account`: typed, private account RPC; login/refresh/E2EE state,
  bounded directory/history, Talk/OpenChat polling and encrypted media transfer.
* `services/line-voice`: one-call media worker, Opus normalization and the pinned
  PLANET transaction-correlation patch. Permanent login material stays with the
  account process. A call worker receives only its call route through stdin.
* `internal/linevoice`: phone WebRTC leg and bounded Opus queues.
* `internal/core/line_*`: authenticated API, native caller binding, account-scoped
  Event Inbox projection and voice handoff.
* `internal/eventinbox/batch.go`: normalized events and encrypted source cursor
  commit in the same transaction. Each source has its own retention quota.

Account metadata has one editable owner in the LINE store. Node does not open
the Go database. The existing Event Inbox owns event retention and checkpoints.
The existing attachment registry/Bridge Drop supplies files; model tools accept
opaque references rather than filesystem paths. Local process isolation is a
fault boundary; the host OS and the approved Keychain helper remain trusted.

## Runtime contract

Account RPC is NDJSON on private stdin/stdout, with protocol version `1`, an
exact account reference, request ID and runtime epoch. Requests are multiplexed
with a bounded pending set. Stale/mismatched replies are rejected. Auth material
is absent from argv, inherited unrelated environment variables, public account
objects and diagnostic logs. Ready/alive are process observations; `health`
performs a live profile/principal check.

The call worker uses protocol `2` and must announce ready before receiving its
single start command. The Go broker consumes a durable intent before obtaining
the route. Native handoffs bind the original conversation, phone, LINE account,
target and a stable call ID; pending reservations expire. A restart or uncertain
invite never causes automatic redial. The initial broker admits one LINE call
at a time on an authority, while account message/event processes remain separate.

The tested runtime is Node 26.7.0; startup currently requires Node major 26.
Dependencies are pinned to LINEJS 3.4.2, types 3.5.0, undici 7.30.0 and thrift
0.23.0. The transport patch checks the exact upstream source SHA before loading.
`scripts/stage_line_runtime.mjs` builds a new artifact, installs production
dependencies from lockfiles and hashes all regular runtime files plus the Node
binary. `LINE_SERVICE_MANIFEST` enables Go verification before a child starts.

## Configuration and packaging

Feature admission is off until `LINE_SERVICE_ENTRY` is configured. Paths are
host configuration; they cannot be supplied by a model tool.

```sh
LINEJS_NODE=/absolute/path/to/node
LINE_SERVICE_ENTRY=/absolute/runtime/line/account/dist/server.js
LINEJS_SHARED_DIR=/absolute/runtime/line/voice
LINEJS_VOICE_WORKER=/absolute/runtime/line/voice/route-worker.mjs
LINE_SERVICE_MANIFEST=/absolute/runtime/line/manifest.json
LINEJS_KEYCHAIN_HELPER=/existing/approved/keychain-helper
LINE_LEGACY_DATA_DIR=/existing/encrypted/account/directory
LINE_SERVICE_ALLOW_WRITE=1
LINE_SERVICE_ALLOW_EVENTS=1
LINE_SERVICE_ALLOW_SCHEDULES=1
LINE_SERVICE_ALLOW_RESOURCES=1
LINEJS_ALLOW_CALLS=0
```

Individual feature gates default to disabled. Calls are separately gated. New
profiles use separate directories and Keychain items. Adopting the existing
profile preserves its absolute directory, Keychain service derivation and
encrypted login file. Read-only LINE probes can still persist normal SDK token
rotation and request-sequence state. A missing key never creates a replacement
key for an existing encrypted database.

```sh
node scripts/stage_line_runtime.mjs /new/staging/line
```

The output must not already exist. `EVERYTHING_GO_INCLUDE_LINE=1` adds this
runtime under `Contents/Resources/line` through the existing signed macOS package
script. Its existing clean-source and signing-identity gates remain in force.
The external Node executable must match the staged manifest. This first rollout
retains the existing approved Keychain helper path.

## User and tool surfaces

The app settings manage accounts, QR pairing, explicit same-account re-login,
paired-device grants, conversation grants, contact/room selection, bounded
history, subscriptions, files, send receipts, schedules, cancellation and
rescheduling. Each send carries an explicit account and account-bound target.
Switching accounts clears the old draft/target and cannot change an existing job.

`POST /api/line/v1/request` requires an exact paired-device bearer credential and
`X-Bridge-Device-ID`; it has no loopback authentication bypass. Pairing and grant
management require the account's original owner. A granted conversation uses
the `bridge_line` native namespace. Caller/session/thread/turn ownership comes
from the actual app-server invocation. Delegated children are excluded. Model
tools cannot pair accounts, grant themselves access, select arbitrary SDK RPCs
or read arbitrary local files.

Fresh QR login stays encrypted in staging. Go checks principal identity and
duplicate binding before committing the login. Same-account re-login preserves
old E2EE key entries and an encrypted previous-login file. A wrong-account scan
discards staging while retaining the original encrypted login.

Native account tools include `accounts`, `status`, `health`, `find_targets`,
`find_openchats`, `get_messages`, `subscribe`, `list_attachments`,
`import_resource`, `download_resource`, `send_message`, `schedule_message`,
`get_operation`, `list_operations`, `cancel_operation`, `reschedule_operation`,
`handoff_call`, `get_call` and `cancel_call`.

## Effect and recovery rules

* `queued`/`scheduled`: persisted intent, no dispatch yet.
* `dispatching`: effect may be in progress.
* `accepted_by_line`: LINE returned an acknowledgement; this is not read/delivery
  confirmation from the recipient.
* `unknown`: retain the original operation and investigate; never retry under a
  new intent automatically.
* `failed_before_effect`: the adapter identified failure before chat dispatch.
* `held_late`/`held_binding`/`held_unavailable`: human review/rescheduling required.

Schedules store UTC milliseconds, IANA timezone, fixed account/principal binding
and revision. The ordinary scheduler tolerance is five seconds; an explicitly
granted late window can extend it. Sleeping/offline hosts do not silently send
late. Cancellation/rescheduling use a receipt revision and refuse ambiguous or
already-dispatched effects. Grant/control/binding changes are checked again
before execution.

History reads do not mark LINE messages read. Event Inbox read/dismiss are local
UI state. Talk and each OpenChat room have separate encrypted checkpoints.
Full-sync responses create an explicit gap record; polling success does not
prove complete historical recovery. Incoming content remains inert and does
not authorize a reply or start an AI turn.

Attachments are capped at 8 MiB, frozen by hash, encrypted in the cache, scoped
to account permissions and retained up to 30 days with an account quota. Images
use the E2EE media path; other types currently send as files. Captions/replies
combined with an attachment are rejected. Upload failure is distinguished from
an unknown chat-send result after upload. Downloads currently resolve a known
message within the bounded recent-message window. Binary content stays out of
model tool results. App clients verify size/hash before opening a download.

Authorized bounded tool results may naturally appear in native runtime history.
The integration does not directly rewrite native JSONL or bulk-inject chat logs.

## Voice acceptance and rollout

The implemented first path retires AI audio, waits for its terminal/cleanup ACK,
then starts the LINE leg on the same phone and conversation. Android preserves
the accepted audio-focus, local-release and foreground-service guards. A failed
release, stale socket, wrong phone, expired reservation or cancellation prevents
handoff. Web uses the same source-binding checks. iPhone original-voice handoff
and answer-first handoff remain disabled until their device prototypes pass.

The call worker starts a monotonic 20-minute timer after remote answer/media
readiness. It maintains silent media when needed and emits bounded structural
diagnostics/frame counts. No audio recording or speech recognition is performed.
The original T103/511 failures remain in evidence; repaired real 60-second and
20-minute calls still require current explicit test authorization and a person
to answer/verify both directions. Unknown media wrappers are counted, not
declared supported.

Before replacing a managed host: record the current signed bundle, launcher,
authority/data/session-store paths and pairing state; preserve the shared native
app-server owner. Package from a clean source snapshot and verify signature,
source hash and runtime manifest. Use the same managed service and authority.
Do not modify other Go instances. Preserve the old bundle and launcher for
rollback. Turning the feature off retains account metadata, encrypted logins,
receipts and unknown effects. It must not replay old calls or messages.

Second-account human pairing is deferred at the user's request in the current
rollout. Two isolated real Node processes and mock two-principal workflows have
passed; this does not replace a real two-account acceptance test.
