# libkeri Interface Design: Three-Pillar Callback Architecture

## Context

Philip Feairheller's vision: libkeri should be a library focused on **escrow and message processing**, with Rust callbacks for events that matter to consumers. The interface surface is three pillars: **Manage local identities**, **Parse KERI streams**, and **Handle callbacks**. This keeps libkeri focused as a library (not an agent/server), letting consumers decide how to handle events like "New Key State", "Missing Signature", "New exn".

## Problem with Current State

- **Three inconsistent cue representations**: `Cue { kin: String, serder }` in kevery.rs, `VecDeque<IndexMap<String, SadValue>>` in habbing.rs `process_cues_iter`, and `ReplyMessageCue { kind: String, data: HashMap }` in revery.rs
- **No typed event system**: consumers poll a `VecDeque` and match on string keys — not idiomatic Rust
- **Escrow methods are `todo!()`**: escrow emit/resolve events don't exist yet
- **No top-level orchestrator**: consumer must wire up Kevery, Parser, Revery, Hab manually

## Recommended Approach: Typed Event Enum + Observer Trait + EventBus

### New Types

**`KeriEvent` enum** — replaces all three cue representations with exhaustive, typed variants:

| Variant | Replaces | Emitted By |
|---------|----------|-----------|
| `KeyStateNew { prefix, state, serder }` | (new) | Kevery after Kever creation |
| `KeyStateUpdated { prefix, state, serder }` | (new) | Kevery after Kever update |
| `ReceiptNeeded { prefix, serder }` | `kin="receipt"` | Kevery.process_event |
| `EventNotice { prefix, serder }` | `kin="notice"` | Kevery.process_event |
| `WitnessNeeded { prefix, serder }` | `kin="witness"` | Kevery.process_event |
| `MissingSignature { prefix, sn, serder, escrow_type }` | (new) | Escrow methods |
| `OutOfOrder { prefix, sn, serder }` | (new) | escrow_oo_event |
| `EscrowResolved { prefix, sn, serder, escrow_type }` | `kin="psUnescrow"` | process_escrows |
| `EscrowTimedOut { prefix, sn, escrow_type }` | (new) | process_escrows |
| `ExchangeReceived { serder, route }` | `kin="saved"` | Parser/Exchanger |
| `ReplyProcessed { serder, route }` | ReplyMessageCue | Revery |
| `DuplicitousEvent { prefix, sn, serder }` | (new) | escrow_ld_event |
| `QueryNotFound { prefix, serder }` | (new) | escrow_query_not_found |

**`EscrowType` enum** — OutOfOrder, PartiallySigned, PartiallyWitnessed, LikelyDuplicitous, UnverifiedWitnessReceipt, UnverifiedReceipt, UnverifiedTransReceipt, KeyStateNotice, QueryNotFound

**`KeriObserver` trait** — consumers implement this. All methods have default no-ops so consumers only override events they care about:
```rust
pub trait KeriObserver: Send + Sync {
    fn on_event(&self, event: &KeriEvent) -> EventAction { EventAction::Continue }
    fn on_key_state_new(&self, prefix: &str, state: &KeyStateRecord, serder: &SerderKERI) -> EventAction { EventAction::Continue }
    fn on_key_state_updated(...) -> EventAction { EventAction::Continue }
    fn on_receipt_needed(...) -> EventAction { EventAction::Continue }
    fn on_exchange(...) -> EventAction { EventAction::Continue }
    fn on_escrowed(...) -> EventAction { EventAction::Continue }
    fn on_escrow_resolved(...) -> EventAction { EventAction::Continue }
    fn on_duplicitous(...) -> EventAction { EventAction::Continue }
}
```

**`EventAction` enum** — `Continue` (library proceeds with default behavior) or `Handled` (consumer takes over). Default is `Continue`.

**`EventBus`** — holds `Vec<Arc<dyn KeriObserver>>`, shared via `Arc` across Kevery/Parser/Revery. `emit(&self, &KeriEvent)` fans out to all observers, calling both `on_event` and the specific typed method.

### Pillar 1: Manage Local Identities

Formalize existing `Hab` methods into an `Identity` trait with config builder structs:
```rust
pub trait Identity {
    fn prefix(&self) -> &str;
    fn state(&self) -> Result<KeyStateRecord, KERIError>;
    fn incept(&mut self, config: InceptionConfig) -> Result<Vec<u8>, KERIError>;
    fn rotate(&mut self, config: RotationConfig) -> Result<Vec<u8>, KERIError>;
    fn interact(&mut self, data: Option<Vec<u8>>) -> Result<Vec<u8>, KERIError>;
    fn sign(&self, data: &[u8]) -> Result<Vec<Siger>, KERIError>;
    fn receipt(&mut self, serder: &SerderKERI) -> Result<Vec<u8>, KERIError>;
    fn replay(&self) -> Result<Vec<u8>, KERIError>;
}
```
`Hab<R>` implements `Identity`. Config structs replace long parameter lists.

### Pillar 2: Parse

Existing `Parser<R>` stays. `Handlers` gains `events: Arc<EventBus>`. Exchange/reply messages emit events directly through the bus in `process_parsed_message`.

### Pillar 3: Handle Callbacks

Consumers implement `KeriObserver`, register via `subscribe()`. Events fire synchronously during `parse()` and `process_escrows()` calls.

### Top-Level Orchestrator

```rust
pub struct Keri {
    db: Arc<Baser>,
    events: Arc<EventBus>,
    habs: HashMap<String, Hab<Vec<u8>>>,
    kevery: Arc<Mutex<Kevery>>,
}

impl Keri {
    pub fn new(db_path: &str, temp: bool) -> Result<Self, KERIError>;
    pub fn subscribe(&mut self, observer: Arc<dyn KeriObserver>);
    pub fn identity(&mut self, name: &str) -> Result<&mut Hab<Vec<u8>>, KERIError>;
    pub fn parse(&mut self, data: &[u8]) -> Result<(), KERIError>;
    pub fn process_escrows(&mut self) -> Result<(), KERIError>;
}
```

### Consumer Usage

```rust
struct MyApp;
impl KeriObserver for MyApp {
    fn on_key_state_new(&self, prefix: &str, state: &KeyStateRecord, _: &SerderKERI) -> EventAction {
        println!("New identity: {prefix}");
        EventAction::Continue
    }
    fn on_exchange(&self, _serder: &dyn Serder, route: &str) -> EventAction {
        println!("Exchange on route: {route}");
        EventAction::Continue
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    libkeri::init()?;
    let mut keri = Keri::new("/tmp/keri-db", true)?;
    keri.subscribe(Arc::new(MyApp));

    let hab = keri.identity("my-aid")?;
    hab.incept(InceptionConfig::default())?;
    keri.parse(&incoming_bytes)?;
    keri.process_escrows()?;
    Ok(())
}
```

## Why This Approach Over Alternatives

**Why trait observers over channels (mpsc)?**
- Channels force an async runtime on every consumer. The trait works sync or async.
- Channels lose type-level dispatch — every receiver filters every event. The trait gives per-event methods with compile-time exhaustiveness.

**Why `EventAction` return?**
- Some consumers want to suppress default behavior (e.g., auto-receipting). Without this, you'd need a separate config mechanism.

**Why `Arc<EventBus>` shared ref?**
- Kevery, Parser, and Revery all emit events. Shared Arc avoids threading callbacks through every method. EventBus is read-only after setup (no mutex needed for emit).

## Implementation Phases

### Phase 1 — Event system (alongside existing cues)
1. Create `src/keri/core/events.rs` with `KeriEvent`, `EscrowType`, `KeriObserver`, `EventAction`, `EventBus`
2. Add `events: Arc<EventBus>` to `Kevery`, `Handlers`, `Revery`
3. At each `cues.push_back()` site in kevery.rs (lines ~241, 247, 258), add parallel `events.emit()`. Keep old cues temporarily.
4. Export new types from `src/lib.rs`

### Phase 2 — Escrow implementation with events
5. Implement escrow `todo!()` stubs in kevery.rs with event emission
6. Add `process_escrows()` to Kevery — walks all escrow types, emits `EscrowResolved`/`EscrowTimedOut`

### Phase 3 — Exchange/reply event integration
7. Emit `ExchangeReceived`/`ReplyProcessed` from parsing.rs `process_parsed_message`
8. Unify `ReplyMessageCue` from revery.rs into `KeriEvent`

### Phase 4 — Identity trait + orchestrator
9. Create `Identity` trait + config structs, implement on `Hab<R>`
10. Create top-level `Keri` orchestrator
11. Remove old `VecDeque<Cue>` and `VecDeque<IndexMap<String, SadValue>>`

## Critical Files

| File | Changes |
|------|---------|
| `src/keri/core/events.rs` | **NEW** — KeriEvent, EscrowType, KeriObserver, EventAction, EventBus |
| `src/keri/core/eventing/kevery.rs` | Replace Cue/VecDeque with Arc<EventBus>, emit typed events, implement escrow methods |
| `src/keri/core/parsing.rs` | Add Arc<EventBus> to Handlers, emit for exchange/reply |
| `src/keri/app/habbing.rs` | Adapt process_cues_iter to KeriEvent, implement Identity trait |
| `src/keri/core/routing/revery.rs` | Replace ReplyMessageCue with KeriEvent, use shared EventBus |
| `src/keri/app/identity.rs` | **NEW** — Identity trait, InceptionConfig, RotationConfig |
| `src/lib.rs` | Export new public types + Keri orchestrator |

## Verification

1. **Unit tests**: Create a `TestObserver` implementing `KeriObserver` that records events in a `Vec<KeriEvent>`. Run through inception/rotation/interaction and assert correct events fire.
2. **Escrow tests**: Feed out-of-order events, verify `OutOfOrder` event fires, then feed the missing event and verify `EscrowResolved` fires.
3. **Integration test**: Wire up `Keri` orchestrator end-to-end: create identity, serialize inception, parse it back, verify `KeyStateNew` callback fires.
4. **Existing tests**: `cargo test` must continue to pass throughout (old cues coexist during migration).
