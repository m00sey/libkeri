use crate::keri::core::serdering::SerderKERI;
use crate::keri::db::basing::KeyStateRecord;
use std::fmt;
use std::sync::Arc;

/// Categorizes the type of escrow an event is held in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EscrowType {
    /// Out-of-order event
    OutOfOrder,
    /// Partially signed event
    PartiallySigned,
    /// Partially witnessed event
    PartiallyWitnessed,
    /// Likely duplicitous event
    LikelyDuplicitous,
    /// Unverified witness receipt
    UnverifiedWitnessReceipt,
    /// Unverified receipt
    UnverifiedReceipt,
    /// Unverified transferable receipt
    UnverifiedTransReceipt,
    /// Key state notice
    KeyStateNotice,
    /// Query not found
    QueryNotFound,
}

impl fmt::Display for EscrowType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EscrowType::OutOfOrder => write!(f, "out-of-order"),
            EscrowType::PartiallySigned => write!(f, "partially-signed"),
            EscrowType::PartiallyWitnessed => write!(f, "partially-witnessed"),
            EscrowType::LikelyDuplicitous => write!(f, "likely-duplicitous"),
            EscrowType::UnverifiedWitnessReceipt => write!(f, "unverified-witness-receipt"),
            EscrowType::UnverifiedReceipt => write!(f, "unverified-receipt"),
            EscrowType::UnverifiedTransReceipt => write!(f, "unverified-trans-receipt"),
            EscrowType::KeyStateNotice => write!(f, "key-state-notice"),
            EscrowType::QueryNotFound => write!(f, "query-not-found"),
        }
    }
}

/// Typed KERI events that flow from the library to the consumer.
/// Each variant carries exactly the data needed to act on it.
#[derive(Debug, Clone)]
pub enum KeriEvent {
    /// A new key state has been verified and accepted for a prefix.
    /// Fires after Kever creation on first inception.
    KeyStateNew {
        prefix: String,
        state: Option<KeyStateRecord>,
        serder: SerderKERI,
    },

    /// An existing key state was updated (rotation, interaction).
    KeyStateUpdated {
        prefix: String,
        state: Option<KeyStateRecord>,
        serder: SerderKERI,
    },

    /// A receipt is needed for a verified event.
    ReceiptNeeded {
        prefix: String,
        serder: SerderKERI,
    },

    /// A notice of a new event that was accepted.
    EventNotice {
        prefix: String,
        serder: SerderKERI,
    },

    /// Witness processing is needed for a locally-sourced event.
    WitnessNeeded {
        prefix: String,
        serder: SerderKERI,
    },

    /// An event is missing required signatures and has been escrowed.
    MissingSignature {
        prefix: String,
        sn: u64,
        escrow_type: EscrowType,
    },

    /// An out-of-order event was received and escrowed.
    OutOfOrder {
        prefix: String,
        sn: u64,
    },

    /// An escrowed event has been resolved (unescrowed successfully).
    EscrowResolved {
        prefix: String,
        sn: u64,
        serder: SerderKERI,
        escrow_type: EscrowType,
    },

    /// An escrowed event timed out and was discarded.
    EscrowTimedOut {
        prefix: String,
        sn: u64,
        escrow_type: EscrowType,
    },

    /// A new exchange message (exn) was received.
    ExchangeReceived {
        route: String,
        said: String,
    },

    /// A reply message was processed.
    ReplyProcessed {
        route: String,
        said: String,
    },

    /// A likely duplicitous event was detected.
    DuplicitousEvent {
        prefix: String,
        sn: u64,
    },

    /// A query response was received.
    QueryResponse {
        prefix: String,
        serder: SerderKERI,
    },

    /// A query could not be fulfilled (no matching data found).
    QueryNotFound {
        prefix: String,
    },
}

/// Controls whether the library proceeds with default handling after the callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventAction {
    /// Continue with default library behavior (e.g., auto-receipt generation).
    Continue,
    /// Suppress the default behavior; the consumer handles it.
    Handled,
}

/// Trait that consumers implement to receive KERI events.
///
/// All methods have default no-op implementations, so consumers
/// only override the events they care about.
pub trait KeriObserver: Send + Sync {
    /// Called for every event. Override for catch-all logging/metrics.
    fn on_event(&self, _event: &KeriEvent) -> EventAction {
        EventAction::Continue
    }

    /// A new identifier's key state was established.
    fn on_key_state_new(
        &self,
        _prefix: &str,
        _state: Option<&KeyStateRecord>,
        _serder: &SerderKERI,
    ) -> EventAction {
        EventAction::Continue
    }

    /// An existing identifier's key state changed.
    fn on_key_state_updated(
        &self,
        _prefix: &str,
        _state: Option<&KeyStateRecord>,
        _serder: &SerderKERI,
    ) -> EventAction {
        EventAction::Continue
    }

    /// A receipt should be generated for a verified event.
    fn on_receipt_needed(&self, _prefix: &str, _serder: &SerderKERI) -> EventAction {
        EventAction::Continue
    }

    /// A notice of a new accepted event.
    fn on_event_notice(&self, _prefix: &str, _serder: &SerderKERI) -> EventAction {
        EventAction::Continue
    }

    /// Witness processing is needed for a locally-sourced event.
    fn on_witness_needed(&self, _prefix: &str, _serder: &SerderKERI) -> EventAction {
        EventAction::Continue
    }

    /// An exchange message was received.
    fn on_exchange(&self, _route: &str, _said: &str) -> EventAction {
        EventAction::Continue
    }

    /// A reply message was processed.
    fn on_reply(&self, _route: &str, _said: &str) -> EventAction {
        EventAction::Continue
    }

    /// An event was escrowed due to missing data.
    fn on_escrowed(&self, _prefix: &str, _sn: u64, _escrow_type: EscrowType) -> EventAction {
        EventAction::Continue
    }

    /// An escrowed event was successfully resolved.
    fn on_escrow_resolved(
        &self,
        _prefix: &str,
        _sn: u64,
        _serder: &SerderKERI,
        _escrow_type: EscrowType,
    ) -> EventAction {
        EventAction::Continue
    }

    /// An escrowed event timed out and was discarded.
    fn on_escrow_timed_out(
        &self,
        _prefix: &str,
        _sn: u64,
        _escrow_type: EscrowType,
    ) -> EventAction {
        EventAction::Continue
    }

    /// A likely duplicitous event was detected.
    fn on_duplicitous(&self, _prefix: &str, _sn: u64) -> EventAction {
        EventAction::Continue
    }
}

/// Distributes KeriEvents to registered observers.
/// Thread-safe, can be shared via Arc across Kevery, Parser, Revery.
pub struct EventBus {
    observers: Vec<Arc<dyn KeriObserver>>,
}

impl EventBus {
    pub fn new() -> Self {
        Self {
            observers: Vec::new(),
        }
    }

    /// Register an observer to receive events.
    pub fn subscribe(&mut self, observer: Arc<dyn KeriObserver>) {
        self.observers.push(observer);
    }

    /// Emit an event to all observers. Returns `Handled` if any observer handled it.
    pub fn emit(&self, event: &KeriEvent) -> EventAction {
        let mut result = EventAction::Continue;

        for observer in &self.observers {
            if observer.on_event(event) == EventAction::Handled {
                result = EventAction::Handled;
            }

            let action = match event {
                KeriEvent::KeyStateNew {
                    prefix,
                    state,
                    serder,
                } => observer.on_key_state_new(prefix, state.as_ref(), serder),
                KeriEvent::KeyStateUpdated {
                    prefix,
                    state,
                    serder,
                } => observer.on_key_state_updated(prefix, state.as_ref(), serder),
                KeriEvent::ReceiptNeeded { prefix, serder } => {
                    observer.on_receipt_needed(prefix, serder)
                }
                KeriEvent::EventNotice { prefix, serder } => {
                    observer.on_event_notice(prefix, serder)
                }
                KeriEvent::WitnessNeeded { prefix, serder } => {
                    observer.on_witness_needed(prefix, serder)
                }
                KeriEvent::ExchangeReceived { route, said } => {
                    observer.on_exchange(route, said)
                }
                KeriEvent::ReplyProcessed { route, said } => observer.on_reply(route, said),
                KeriEvent::MissingSignature {
                    prefix,
                    sn,
                    escrow_type,
                } => observer.on_escrowed(prefix, *sn, *escrow_type),
                KeriEvent::OutOfOrder { prefix, sn } => {
                    observer.on_escrowed(prefix, *sn, EscrowType::OutOfOrder)
                }
                KeriEvent::EscrowResolved {
                    prefix,
                    sn,
                    serder,
                    escrow_type,
                } => observer.on_escrow_resolved(prefix, *sn, serder, *escrow_type),
                KeriEvent::EscrowTimedOut {
                    prefix,
                    sn,
                    escrow_type,
                } => observer.on_escrow_timed_out(prefix, *sn, *escrow_type),
                KeriEvent::DuplicitousEvent { prefix, sn } => {
                    observer.on_duplicitous(prefix, *sn)
                }
                KeriEvent::QueryResponse { .. } | KeriEvent::QueryNotFound { .. } => {
                    EventAction::Continue
                }
            };

            if action == EventAction::Handled {
                result = EventAction::Handled;
            }
        }

        result
    }

    /// Returns true if there are no registered observers.
    pub fn is_empty(&self) -> bool {
        self.observers.is_empty()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for EventBus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventBus")
            .field("observer_count", &self.observers.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingObserver {
        count: AtomicUsize,
    }

    impl CountingObserver {
        fn new() -> Self {
            Self {
                count: AtomicUsize::new(0),
            }
        }

        fn count(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }
    }

    impl KeriObserver for CountingObserver {
        fn on_event(&self, _event: &KeriEvent) -> EventAction {
            self.count.fetch_add(1, Ordering::SeqCst);
            EventAction::Continue
        }
    }

    struct HandlingObserver;

    impl KeriObserver for HandlingObserver {
        fn on_exchange(&self, _route: &str, _said: &str) -> EventAction {
            EventAction::Handled
        }
    }

    #[test]
    fn test_event_bus_no_observers() {
        let bus = EventBus::new();
        assert!(bus.is_empty());
        let action = bus.emit(&KeriEvent::OutOfOrder {
            prefix: "test".to_string(),
            sn: 0,
        });
        assert_eq!(action, EventAction::Continue);
    }

    #[test]
    fn test_event_bus_counting_observer() {
        let mut bus = EventBus::new();
        let observer = Arc::new(CountingObserver::new());
        bus.subscribe(observer.clone());

        bus.emit(&KeriEvent::OutOfOrder {
            prefix: "test".to_string(),
            sn: 0,
        });
        bus.emit(&KeriEvent::DuplicitousEvent {
            prefix: "test".to_string(),
            sn: 1,
        });

        assert_eq!(observer.count(), 2);
    }

    #[test]
    fn test_event_bus_handled_action() {
        let mut bus = EventBus::new();
        bus.subscribe(Arc::new(HandlingObserver));

        // HandlingObserver returns Handled for ExchangeReceived
        let action = bus.emit(&KeriEvent::ExchangeReceived {
            route: "/test".to_string(),
            said: "SAID123".to_string(),
        });
        assert_eq!(action, EventAction::Handled);

        // But Continue for other events
        let action = bus.emit(&KeriEvent::OutOfOrder {
            prefix: "test".to_string(),
            sn: 0,
        });
        assert_eq!(action, EventAction::Continue);
    }

    #[test]
    fn test_multiple_observers() {
        let mut bus = EventBus::new();
        let counter = Arc::new(CountingObserver::new());
        bus.subscribe(counter.clone());
        bus.subscribe(Arc::new(HandlingObserver));

        // Both observers see the event
        let action = bus.emit(&KeriEvent::ExchangeReceived {
            route: "/test".to_string(),
            said: "SAID123".to_string(),
        });

        assert_eq!(counter.count(), 1);
        assert_eq!(action, EventAction::Handled);
    }

    #[test]
    fn test_escrow_type_display() {
        assert_eq!(EscrowType::OutOfOrder.to_string(), "out-of-order");
        assert_eq!(EscrowType::PartiallySigned.to_string(), "partially-signed");
        assert_eq!(
            EscrowType::LikelyDuplicitous.to_string(),
            "likely-duplicitous"
        );
    }
}
