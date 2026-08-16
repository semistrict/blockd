//! Tokio channels used by both production and simulation actors.

pub use tokio::sync::mpsc::error::{TryRecvError, TrySendError};
pub use tokio::sync::mpsc::{
    Receiver, Sender, UnboundedReceiver, UnboundedSender, channel as bounded,
    unbounded_channel as unbounded,
};
pub use tokio::sync::oneshot::channel as oneshot;
pub use tokio::sync::oneshot::error::RecvError as Closed;
pub use tokio::sync::oneshot::{Receiver as OneReceiver, Sender as OneSender};
