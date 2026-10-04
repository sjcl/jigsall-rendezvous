//! Byte reservations travel with each frame, including while a writer is sending
//! it. Dropping a queue, a failed enqueue or a cancelled write returns both caps.
use crate::protocol::{Frame, ServerMessage};
use axum::extract::ws::Utf8Bytes;
use std::sync::Arc;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub(crate) struct Sender {
    tx: mpsc::Sender<Packet>,
    bytes: Arc<Semaphore>,
    global: Arc<Semaphore>,
}
pub(crate) struct Packet {
    pub text: Utf8Bytes,
    // Keep this alive until the write finishes, not merely until dequeue.
    pub reservation: Reservation,
}
pub(crate) struct Reservation {
    _connection: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
}
pub(crate) fn channel(
    messages: usize,
    bytes: usize,
    global: Arc<Semaphore>,
) -> (Sender, mpsc::Receiver<Packet>) {
    let (tx, rx) = mpsc::channel(messages);
    (
        Sender {
            tx,
            bytes: Arc::new(Semaphore::new(bytes)),
            global,
        },
        rx,
    )
}
impl Sender {
    pub fn try_send(&self, message: ServerMessage) -> Result<(), ()> {
        let slot = self.tx.try_reserve().map_err(|_| ())?;
        // Shrink away serializer spare capacity so charged bytes equal the
        // retained UTF-8 allocation. Control frames also consume the budget.
        let json = serde_json::to_string(&Frame::new(message))
            .expect("protocol serialization")
            .into_boxed_str()
            .into_string();
        let bytes = u32::try_from(json.len()).map_err(|_| ())?;
        let connection = self
            .bytes
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| ())?;
        let global = self
            .global
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| ())?;
        slot.send(Packet {
            text: json.into(),
            reservation: Reservation {
                _connection: connection,
                _global: global,
            },
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{encode_signal, parse_server, PeerId, MAX_SIGNAL_BYTES};
    fn signal() -> ServerMessage {
        ServerMessage::Signal {
            from_peer_id: PeerId([1; 16]),
            payload_base64: encode_signal(&vec![7; MAX_SIGNAL_BYTES]),
        }
    }
    fn cost() -> usize {
        serde_json::to_string(&Frame::new(signal())).unwrap().len()
    }
    #[test]
    fn per_connection_budget_includes_writer_and_returns_on_every_drop_path() {
        let global = Arc::new(Semaphore::new(cost() * 10));
        let (tx, mut rx) = channel(128, cost() * 2, global.clone());
        tx.try_send(signal()).unwrap();
        tx.try_send(signal()).unwrap();
        assert!(tx.try_send(signal()).is_err());
        let in_flight = rx.try_recv().unwrap();
        assert_eq!(parse_server(&in_flight.text).unwrap(), signal());
        assert!(tx.try_send(signal()).is_err());
        drop(in_flight); // completed or cancelled write
        tx.try_send(signal()).unwrap();
        drop(rx); // disconnect: queued frames return all reservations
        assert_eq!(global.available_permits(), cost() * 10);
        assert!(tx.try_send(signal()).is_err()); // receiver already closed
        assert_eq!(global.available_permits(), cost() * 10);
    }
    #[test]
    fn global_budget_spans_targets_and_failed_reservation_returns_local_bytes() {
        let global = Arc::new(Semaphore::new(cost() * 2));
        let (a, a_rx) = channel(128, cost() * 2, global.clone());
        let (b, mut b_rx) = channel(128, cost() * 2, global.clone());
        a.try_send(signal()).unwrap();
        a.try_send(signal()).unwrap();
        for _ in 0..3 {
            assert!(b.try_send(signal()).is_err());
            assert_eq!(b.bytes.available_permits(), cost() * 2);
        }
        drop(a_rx);
        b.try_send(signal()).unwrap();
        b.try_send(signal()).unwrap();
        drop(b_rx.try_recv().unwrap());
        drop(b_rx);
        assert_eq!(global.available_permits(), cost() * 2);
    }
    #[test]
    fn message_count_and_control_bytes_are_bounded_too() {
        let global = Arc::new(Semaphore::new(cost() * 3));
        let (tx, rx) = channel(1, cost() * 3, global.clone());
        tx.try_send(signal()).unwrap();
        assert!(tx.try_send(ServerMessage::RoomClosed {}).is_err());
        assert_eq!(global.available_permits(), cost() * 2);
        drop(rx);
        let (tx, rx) = channel(128, 1, global.clone());
        assert!(tx.try_send(ServerMessage::RoomClosed {}).is_err());
        drop(rx);
        assert_eq!(global.available_permits(), cost() * 3);
    }
}
