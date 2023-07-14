pub fn get_random_node_id() -> String {
    let rnd_part = short_random_id(8);
    format!("{}", rnd_part)
}

pub fn get_queue_id(node_id: &str, queue_name: &str) -> String {
    let queue_id = format!("{}-{}", node_id, queue_name);
    return queue_id;
}

// use base64::{engine::general_purpose, Engine as _};
use rand::Rng;

pub fn short_random_id(nchars: i8) -> String {
    let mut rng = rand::thread_rng();
    let random_bytes: Vec<u8> = (0..nchars).map(|_| rng.gen()).collect();
    let encoded = bs58::encode(random_bytes).into_string();
    encoded
    // general_purpose::URL_SAFE_NO_PAD.encode(&random_bytes)
}