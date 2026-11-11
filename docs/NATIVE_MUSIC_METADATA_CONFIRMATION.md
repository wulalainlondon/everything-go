# Native Music metadata confirmation

Android official YouTube Music sometimes leaves `mediaId` empty. An empty ID
remains empty in every observation and receipt. It is never replaced with the
requested URL ID.

The native SDK may issue a version 1 `nativeMatch` with basis `metadata_v1` only
from a fresh, post-dispatch, confirmed operation callback. Go accepts this only
for `ytmusic_android`, the original operation and action, a nonempty binding,
fresh confirmed causal receipt, and matching metadata. iOS and legacy Android
receipts retain their existing ID requirements.

For play, `expectedMediaId` is the requested canonical YouTube Music URL ID,
explicitly an expectation. Title and complete artist credits must match. Only a
bounded terminal `(Official Video)`, `[Official Music Video]` or official-audio
presentation suffix is ignored; live/remix/acoustic/cover/version qualifiers
remain significant. Performer credits may reorder and use the supported comma,
ampersand, ideographic-comma, spaced `and` or spaced `和` separators. Missing,
extra and duplicate credits do not match; no localized aliases are guessed.
The SDK's legacy single-credit display matcher cannot issue this stronger proof.

For resume, seek, next and previous, the evidence carries the exact binding,
media ID (possibly empty), title and artist observed before dispatch. Go compares
these to its immutable fresh pre-admission owned snapshot. Resume/seek require
the same metadata; next/previous require a real complete metadata transition in
the same binding, never a punctuation/presentation change or a known same ID.
Action-specific playback/position requirements remain enforced.

Snapshots, read replies and historical/replayed replies carry no active
`nativeMatch`. Original causal evidence is retained separately and cannot be
upgraded by a later snapshot or contradictory late original-command feedback after its terminal receipt is sealed. Unknown reservations, foreign-item adoption,
source/revision/owner checks and no-replay behavior are unchanged.

Both repaired Android and Go source are required for empty-ID causal success.
An older Go continues to reject such success; this is conservative compatibility,
not permission to retry. Source tests do not certify installation or human audio.
Canonical parity vectors: `contracts/native-music/metadata-v1.json` in the parent
workspace, copied byte-for-byte into both repositories' test fixtures.
