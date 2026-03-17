mod codec;
mod path;
mod protocol;
mod receiver;
mod sender;
mod shared;
mod tasks;

const BLOCK_SIZE: u64 = 128 * 1024;
const BLOCK_SIZE_USIZE: usize = 128 * 1024;
const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;

pub use codec::PxsCodec;
pub use protocol::{
    Block, FileMetadata, Message, apply_file_metadata, deserialize_message, serialize_message,
};
pub use receiver::{
    handle_client, run_pull_client, run_receiver, run_ssh_receiver, run_stdio_receiver,
};
pub use sender::{
    run_sender, run_sender_listener, run_ssh_sender, run_stdio_sender, sync_remote_file,
};
