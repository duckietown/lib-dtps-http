use std::collections::HashMap;
use std::sync::Arc;

use maplit::hashmap;
use serde_yaml;
use tokio::sync::Mutex;

use crate::utils::{divide_in_components, is_prefix_of};
use crate::ServerState;

#[derive(Debug, Clone)]
pub enum Transforms {
    GetInside(Vec<String>),
}

#[derive(Debug, Clone)]
pub struct SourceComposition {
    compose: HashMap<Vec<String>, Box<TypeOFSource>>,
}

#[derive(Debug, Clone)]
pub enum TypeOFSource {
    NodeRoot,
    ForwardedQueue(String),
    OurQueue(String),
    Compose(SourceComposition),
    Transformed(Box<TypeOFSource>, Transforms),
}

// #[derive(Debug, Clone)]
// struct MakeSignal {
//     source_type: TypeOFSource,
//     transforms: Transforms,
// }

pub trait ResolveDataSingle {
    fn resolve_data_single(&self, state: &ServerState) -> Result<serde_cbor::Value, String>;
}

pub async fn interpret_path(
    path_components: &Vec<String>,
    ss_mutex: Arc<Mutex<ServerState>>,
) -> Result<TypeOFSource, String> {
    // look for all the keys in our queues
    let keys: Vec<String> = {
        let ss = ss_mutex.lock().await;
        ss.oqs.keys().cloned().collect()
    };
    log::debug!(" = path_components = {:?} ", path_components);
    let mut subtopics: Vec<(String, Vec<String>, Vec<String>)> = vec![];
    let mut subtopics_vec = vec![];
    for k in &keys {
        let topic_components = divide_in_components(&k, '.');

        subtopics_vec.push(topic_components.clone());

        if topic_components.len() == 0 {
            continue;
        }

        match is_prefix_of(&topic_components, &path_components) {
            None => {}
            Some((_, rest)) => {
                let source_type = TypeOFSource::OurQueue(k.clone());
                return if rest.len() == 0 {
                    Ok(source_type)
                } else {
                    Ok(TypeOFSource::Transformed(
                        Box::new(source_type),
                        Transforms::GetInside(rest),
                    ))
                };
            }
        };

        // log::debug!("k: {:?} = components = {:?} ", k, components);

        let (matched, rest) = match is_prefix_of(&path_components, &topic_components) {
            None => continue,

            Some(rest) => rest,
        };

        subtopics.push((k.clone(), matched, rest));
    }

    eprintln!("subtopics: {:?}", subtopics);
    if subtopics.len() == 0 {
        let s = format!(
            "Cannot find a matching topic for {:?}.\nMy Topics: {:?}\n",
            path_components, subtopics_vec
        );
        return Err(s);
    }

    let mut sc = SourceComposition {
        compose: hashmap! {},
    };

    for (topic_name, _, rest) in subtopics {
        let source_type = TypeOFSource::OurQueue(topic_name.clone());
        sc.compose.insert(rest.clone(), Box::new(source_type));
    }

    Ok(TypeOFSource::Compose(sc))
}
