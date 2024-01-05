use async_trait::async_trait;

use crate::signals_logic::{Callable, TypeOFSource};
use crate::{not_implemented, RawData, ResolvedData, ServerStateAccess, DTPSR};

#[async_trait]
impl Callable for TypeOFSource {
    async fn call(&self, _presented_as: &str, _ssa: ServerStateAccess, _data: &RawData) -> DTPSR<ResolvedData> {
        // debug_with_info!("patching {self:#?} with {patch:#?}");

        not_implemented!("call for {self:#?} with {self:?}") // TODO: implement Call for Rust
    }
}
