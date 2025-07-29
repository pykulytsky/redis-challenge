use std::time::{SystemTime, UNIX_EPOCH};

pub fn get_epoch_ms() -> usize {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as usize
}
