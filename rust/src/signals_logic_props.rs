use crate::{DataProps, SourceComposition, TopicProperties, TypeOFSource};

impl DataProps for TypeOFSource {
    fn get_properties(&self) -> TopicProperties {
        match self {
            TypeOFSource::Digest(..) => TopicProperties {
                streamable: false,
                pushable: false,
                readable: true,
                immutable: true,
                has_history: false,
                patchable: false,
            },
            TypeOFSource::ForwardedQueue(q) => q.properties.clone(),
            TypeOFSource::OurQueue(_, props) => props.clone(),

            TypeOFSource::Compose(sc) => sc.get_properties(),
            TypeOFSource::Transformed(s, _) => s.get_properties(), // ok but when do we do the history?
            TypeOFSource::Deref(d) => d.get_properties(),
            TypeOFSource::OtherProxied(_) => {
                TopicProperties {
                    streamable: false,
                    pushable: false,
                    readable: true,
                    immutable: false, // maybe we can say it true sometime
                    has_history: false,
                    patchable: false,
                }
            }
            TypeOFSource::MountedDir(_, _, props) => props.clone(),
            TypeOFSource::MountedFile { properties, .. } => properties.clone(),
            TypeOFSource::Index(s) => s.get_properties(),
            TypeOFSource::Aliased(_, _) => {
                todo!("get_properties for {self:#?} with {self:?}")
            }
            TypeOFSource::History(_) => {
                TopicProperties {
                    streamable: false,
                    pushable: false,
                    readable: true,
                    immutable: false, // maybe we can say it true sometime
                    has_history: false,
                    patchable: false,
                }
            }
        }
    }
}

impl DataProps for SourceComposition {
    fn get_properties(&self) -> TopicProperties {
        let pushable = false;

        let mut immutable = true;
        let mut streamable = false;
        let mut readable = true;
        let has_history = false;
        let patchable = true;

        for (_k, v) in self.compose.iter() {
            let p = v.get_properties();

            // immutable: ALL immutable
            immutable = immutable && p.immutable;
            // streamable: ANY streamable
            streamable = streamable || p.streamable;
            // readable: ALL readable
            readable = readable && p.readable;
            // patchable: ANY patchable
            // patchable = patchable || p.patchable;
        }

        TopicProperties {
            streamable,
            pushable,
            readable,
            immutable,
            has_history,
            patchable,
        }
    }
}
