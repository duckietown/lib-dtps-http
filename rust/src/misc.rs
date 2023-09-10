use rand::Rng;

use crate::TopicName;

pub fn get_random_node_id() -> String {
    let rnd_part = short_random_id(8);
    format!("{}", rnd_part)
}

pub fn get_queue_id(node_id: &str, topic_name: &TopicName) -> String {
    let queue_name = topic_name.as_relative_url();
    if queue_name == "" {
        return node_id.to_string();
    }
    let queue_id = format!("{}:{}", node_id, queue_name);
    return queue_id;
}

pub fn short_random_id(nchars: i8) -> String {
    let mut rng = rand::thread_rng();
    let random_bytes: Vec<u8> = (0..nchars).map(|_| rng.gen()).collect();
    let encoded = bs58::encode(random_bytes).into_string();
    encoded
}
