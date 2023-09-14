use std::{
    collections::HashMap,
    path::PathBuf,
};

use anyhow::Context;
use async_recursion::async_recursion;
use maplit::hashmap;

use crate::{
    context,
    debug_with_info,
    divide_in_components,
    error_with_info,
    is_prefix_of,
    not_implemented,
    utils,
    DTPSError,
    ForwardedQueue,
    OtherProxied,
    ServerState,
    SourceComposition,
    TopicName,
    TopicProperties,
    Transforms,
    TypeOFSource,
    DTPSR,
    REL_URL_META,
    URL_HISTORY,
};

#[async_recursion]
pub async fn interpret_path(
    path: &str,
    query: &HashMap<String, String>,
    referrer: &Option<String>,
    ss: &ServerState,
) -> DTPSR<TypeOFSource> {
    let _ = referrer;
    // debug_with_info!("interpret_path: path: {}", path);
    let path_components0 = divide_in_components(path, '/');
    let path_components = path_components0.clone();
    let ends_with_dash = path.ends_with('/');

    if path_components.contains(&"!".to_string()) {
        return DTPSError::other(format!("interpret_path: Cannot have ! in path: {:?}", path_components));
    }
    {
        let deref: String = ":deref".to_string();

        if path_components.contains(&deref) {
            let i = path_components.iter().position(|x| x == &deref).unwrap();
            let before = path_components.get(0..i).unwrap().to_vec();
            let after = path_components.get(i + 1..).unwrap().to_vec();
            // debug_with_info!("interpret_path: before: {:?} after: {:?}", before, after);

            let before2 = before.join("/") + "/";
            let interpret_before = interpret_path(&before2, query, referrer, ss).await?;

            let first_part = match interpret_before {
                TypeOFSource::Compose(sc) => TypeOFSource::Deref(sc),
                _ => {
                    return DTPSError::other(format!(
                        "interpret_path: deref: before is not Compose: {:?}",
                        interpret_before
                    ));
                }
            };
            return resolve_extra_components(&first_part, &after, ends_with_dash);
        }
    }
    {
        let history_marker: String = URL_HISTORY.to_string();

        if path_components.contains(&history_marker) {
            let i = path_components.iter().position(|x| x == &history_marker).unwrap();
            let before = path_components.get(0..i).unwrap().to_vec();
            let after = path_components.get(i + 1..).unwrap().to_vec();
            // debug_with_info!("interpret_path: before: {:?} after: {:?}", before, after);

            let before2 = before.join("/") + "/";
            let interpret_before = interpret_path(&before2, query, referrer, ss).await?;

            let first_part = TypeOFSource::History(Box::new(interpret_before));
            return resolve_extra_components(&first_part, &after, ends_with_dash);
        }
    }
    {
        let index_marker = REL_URL_META.to_string();
        if path_components.contains(&index_marker) {
            let i = path_components.iter().position(|x| x == &index_marker).unwrap();
            let before = path_components.get(0..i).unwrap().to_vec();
            let after = path_components.get(i + 1..).unwrap().to_vec();

            let before2 = before.join("/") + "/";
            let interpret_before = interpret_path(&before2, query, referrer, ss).await?;

            let first_part = TypeOFSource::Index(Box::new(interpret_before));
            return resolve_extra_components(&first_part, &after, ends_with_dash);
        }
    }

    if path_components.len() > 1 && path_components.first().unwrap() == ":ipfs" {
        return if path_components.len() != 3 {
            DTPSError::other(format!("Wrong number of components: {:?}; expected 3", path_components))
        } else {
            let digest = path_components.get(1).unwrap();
            let content_type = path_components.get(2).unwrap();
            let content_type = content_type.replace('_', "/");
            Ok(TypeOFSource::Digest(digest.to_string(), content_type))
        };
    }

    {
        for (mounted_at, info) in &ss.proxied_other {
            if let Some((_, b)) = is_prefix_of(mounted_at.as_components(), &path_components) {
                let mut path_and_query = b.join("/");
                if ends_with_dash && !b.is_empty() {
                    path_and_query.push('/');
                }
                path_and_query.push_str(&utils::format_query(query));

                let other = OtherProxied {
                    // reached_at: mounted_at.clone(),
                    path_and_query,
                    op: info.clone(),
                };
                return Ok(TypeOFSource::OtherProxied(other));
            }
        }
    }

    let all_sources = iterate_type_of_sources(ss, true);
    resolve(&ss.node_id, &path_components, ends_with_dash, &all_sources)
}

fn resolve(
    origin_node: &str,
    path_components: &Vec<String>,
    ends_with_dash: bool,
    all_sources: &[(TopicName, TypeOFSource)],
) -> DTPSR<TypeOFSource> {
    let mut subtopics: Vec<(TopicName, Vec<String>, Vec<String>, TypeOFSource)> = vec![];
    let mut subtopics_vec = vec![];

    // if path_components.len() == 0 && ends_with_dash {
    //     p =
    //     return Ok(TypeOFSource::OurQueue(TopicName::root(), p));
    // }
    // debug_with_info!(" = all_sources =\n{:#?} ", all_sources);
    for (k, source) in all_sources.iter() {
        let topic_components = k.as_components();
        subtopics_vec.push(topic_components.clone());

        if topic_components.is_empty() {
            continue;
        }

        match is_prefix_of(topic_components, path_components) {
            None => {}
            Some((_, rest)) => {
                return resolve_extra_components(source, &rest, ends_with_dash);
            }
        };

        // debug_with_info!("k: {:?} = components = {:?} ", k, components);

        let (matched, rest) = match is_prefix_of(path_components, topic_components) {
            None => continue,

            Some(rest) => rest,
        };
        if !ends_with_dash {}
        // debug_with_info!("pushing: {k:?}");
        subtopics.push((k.clone(), matched, rest, source.clone()));
    }

    // debug_with_info!("subtopics: {subtopics:?}");
    if subtopics.is_empty() {
        let s = format!(
            "Cannot find a matching topic for {:?}.\nMy Topics: {:?}\n",
            path_components, subtopics_vec
        );
        error_with_info!("{}", s);
        return Err(DTPSError::TopicNotFound(s));
    }
    if !ends_with_dash {
        let s = "Expected to end with dash".to_string();
        return Err(DTPSError::TopicNotFound(s));
    }
    let y = path_components.join("/");
    let unique_id = format!("{origin_node}:{y}");
    let topic_name = TopicName::from_components(path_components);
    let mut sc = SourceComposition {
        topic_name,
        compose: hashmap! {},
        unique_id,
        origin_node: origin_node.to_string(),
    };
    for (_, _, rest, source) in subtopics {
        sc.compose.insert(rest.clone(), Box::new(source));
    }
    Ok(TypeOFSource::Compose(sc))
}

pub fn iterate_type_of_sources(s: &ServerState, add_aliases: bool) -> Vec<(TopicName, TypeOFSource)> {
    let mut res: Vec<(TopicName, TypeOFSource)> = vec![];
    for (topic_name, x) in s.proxied_topics.iter() {
        let fq = ForwardedQueue {
            subscription: x.from_subscription.clone(),
            his_topic_name: x.its_topic_name.clone(),
            my_topic_name: topic_name.clone(),
            properties: x.tr_original.properties.clone(),
        };
        res.push((topic_name.clone(), TypeOFSource::ForwardedQueue(fq)));
    }

    for (topic_name, oq) in s.oqs.iter() {
        res.push((
            topic_name.clone(),
            TypeOFSource::OurQueue(topic_name.clone(), oq.tr.properties.clone()),
        ));
    }

    if add_aliases {
        let sources_no_aliases = iterate_type_of_sources(s, false);

        for (topic_name, new_topic) in s.aliases.iter() {
            let resolved = resolve(&s.node_id, new_topic.as_components(), false, &sources_no_aliases);
            let r = match resolved {
                Ok(rr) => rr,
                Err(e) => {
                    let s1 = topic_name.as_dash_sep();
                    let s2 = new_topic.as_dash_sep();
                    error_with_info!("Cannot resolve alias of {s1} -> {s2} yet:\n{e}");
                    continue;
                }
            };
            res.push((topic_name.clone(), r));
        }
    }

    for (topic_name, oq) in s.local_dirs.iter() {
        let props = TopicProperties {
            streamable: false,
            pushable: false,
            readable: true,
            immutable: false,
            has_history: false,
            patchable: false,
        };
        res.push((
            topic_name.clone(),
            TypeOFSource::MountedDir(topic_name.clone(), oq.local_dir.clone(), props),
        ));
    }
    // sort res by length of topic_name
    res.sort_by(|a, b| a.0.as_components().len().cmp(&b.0.as_components().len()));
    // now reverse
    res.reverse();
    res
}

fn resolve_extra_components(source: &TypeOFSource, rest: &Vec<String>, ends_with_dash: bool) -> DTPSR<TypeOFSource> {
    let mut cur = source.clone();
    for a in rest.iter() {
        cur = context!(
            cur.get_inside(a, ends_with_dash),
            "interpret_path: cannot match for {a:?} rest: {rest:?}",
        )?;
    }
    Ok(cur)
}

impl TypeOFSource {
    pub fn get_inside(&self, s: &str, ends_with_dash: bool) -> DTPSR<Self> {
        match self {
            TypeOFSource::ForwardedQueue(_q) => Ok(TypeOFSource::Transformed(
                Box::new(self.clone()),
                Transforms::GetInside(vec![s.to_string()]),
            )),
            TypeOFSource::OurQueue(..) => {
                // TODO: this is where you would check the schema
                if !ends_with_dash {
                    return Err(DTPSError::TopicNotFound(format!(
                        "Expected to end with dash: {:#?}",
                        self
                    )));
                }
                Ok(TypeOFSource::Transformed(
                    Box::new(self.clone()),
                    Transforms::GetInside(vec![s.to_string()]),
                ))
            }
            TypeOFSource::MountedDir(topic_name, path, props) => {
                let p = PathBuf::from(&path);
                let inside = p.join(s);
                if inside.exists() {
                    if inside.is_dir() {
                        if !ends_with_dash {
                            return Err(DTPSError::TopicNotFound(format!(
                                "Directory expected to end with dash: {:#?}",
                                self
                            )));
                        }
                        Ok(TypeOFSource::MountedDir(
                            topic_name.clone(),
                            inside.to_str().unwrap().to_string(),
                            props.clone(),
                        ))
                    } else if inside.is_file() {
                        Ok(TypeOFSource::MountedFile {
                            topic_name: topic_name.clone(),
                            filename: inside.to_str().unwrap().to_string(),

                            properties: props.clone(),
                        })
                    } else {
                        not_implemented!("get_inside for {self:#?} with {s:?}")
                    }
                } else {
                    Err(DTPSError::TopicNotFound(format!(
                        "File not found: {}",
                        p.join(s).to_str().unwrap()
                    )))
                }

                // not_implemented!("get_inside for {self:#?} with {s:?}")
            }
            TypeOFSource::Compose(_c) => {
                not_implemented!("get_inside for {self:#?} with {s:?}")
            }
            TypeOFSource::Transformed(source, transform) => {
                // not_implemented!("get_inside for {self:#?} with {s:?}")
                Ok(TypeOFSource::Transformed(source.clone(), transform.get_inside(s)))
            }

            TypeOFSource::MountedFile { .. } => {
                let v = vec![s.to_string()];
                let transform = Transforms::GetInside(v);
                let tr = TypeOFSource::Transformed(Box::new(self.clone()), transform);
                Ok(tr)
            }
            TypeOFSource::Index(_) => {
                not_implemented!("get_inside for {self:#?} with {s:?}")
            }
            TypeOFSource::Digest(_, _) => {
                not_implemented!("get_inside for {self:#?} with {s:?}")
            }
            TypeOFSource::Deref(_c) => Ok(TypeOFSource::Transformed(
                Box::new(self.clone()),
                Transforms::GetInside(vec![s.to_string()]),
            )),
            TypeOFSource::OtherProxied(_) => {
                not_implemented!("get_inside for {self:#?} with {s:?}")
            }
            TypeOFSource::Aliased(_, _) => Ok(TypeOFSource::Transformed(
                Box::new(self.clone()),
                Transforms::GetInside(vec![s.to_string()]),
            )),
            TypeOFSource::History(_) => Ok(TypeOFSource::Transformed(
                Box::new(self.clone()),
                Transforms::GetInside(vec![s.to_string()]),
            )),
        }
    }
}
