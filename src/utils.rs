use tokio::fs::OpenOptions;

pub fn rw_open_options() -> OpenOptions {
    let mut open_options = OpenOptions::new();
    open_options.read(true).write(true).create(true);

    open_options
}

pub fn r_open_options() -> OpenOptions {
    let mut open_options = OpenOptions::new();
    open_options.read(true);
    open_options
}

pub async fn truncate(filename: &str) {
    let mut options = rw_open_options();
    options.truncate(true);

    options
        .open(filename)
        .await
        .unwrap_or_else(|_| panic!("failed to truncate file: {}", filename));
}

pub async fn delete(filename: &str) {
    tokio::fs::remove_file(filename)
        .await
        .unwrap_or_else(|_| panic!("failed to delete file: {}", filename));
}

#[cfg(test)]
pub mod tests {
    use rand::Rng;
    use std::iter;

    pub fn generate_random_node_id() -> String {
        const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        let mut rng = rand::thread_rng();
        let len: usize = rng.gen_range(1..5);
        let one_char = || CHARSET[rng.gen_range(0..CHARSET.len())] as char;
        iter::repeat_with(one_char).take(len).collect()
    }
}
