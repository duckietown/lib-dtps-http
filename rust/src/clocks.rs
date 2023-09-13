use std::{
    collections::{
        HashMap,
        HashSet,
    },
    fmt,
    fmt::Display,
    ops::Add,
    path::PathBuf,
};

use bytes::Bytes;
use derive_more::Constructor;
use maplit::hashmap;
use schemars::{
    schema::RootSchema,
    JsonSchema,
};
use serde::{
    Deserialize,
    Serialize,
};
use serde_cbor::Value as CBORValue;
use sha256::digest;
use url::Url;

use crate::{
    divide_in_components,
    identify_presentation,
    join_con,
    join_ext,
    parse_url_ext,
    ContentPresentation,
    DTPSError,
    LinkBenchmark,
    TopicName,
    CONTENT_TYPE_CBOR,
    CONTENT_TYPE_JSON,
    DTPSR,
};

#[derive(Serialize, Deserialize, Debug, Clone, Constructor, PartialEq)]
pub struct MinMax<T: Ord + Clone + PartialEq> {
    pub min: T,
    pub max: T,
}

pub fn merge_minmax<T: Ord + Clone>(minmax1: &MinMax<T>, minmax2: &MinMax<T>) -> MinMax<T> {
    let mut min = minmax1.min.clone();
    let mut max = minmax1.max.clone();
    if minmax2.min < min {
        min = minmax2.min.clone();
    }
    if minmax2.max > max {
        max = minmax2.max.clone();
    }
    MinMax::new(min, max)
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Constructor, PartialEq)]
pub struct Clocks {
    pub logical: HashMap<String, MinMax<usize>>,
    pub wall: HashMap<String, MinMax<i64>>,
}

pub fn merge_to<T: Ord + Clone>(x: &mut HashMap<String, MinMax<T>>, y: &HashMap<String, MinMax<T>>) {
    for (key, minmax) in y {
        if let Some(minmax2) = x.get(key) {
            x.insert(key.clone(), merge_minmax(minmax, minmax2));
        } else {
            x.insert(key.clone(), minmax.clone());
        }
    }
}

pub fn merge_clocks(clock1: &Clocks, clock2: &Clocks) -> Clocks {
    let mut logical = HashMap::new();
    let mut wall = HashMap::new();

    merge_to(&mut logical, &clock1.logical);
    merge_to(&mut logical, &clock2.logical);
    merge_to(&mut wall, &clock1.wall);
    merge_to(&mut wall, &clock2.wall);

    Clocks::new(logical, wall)
}
