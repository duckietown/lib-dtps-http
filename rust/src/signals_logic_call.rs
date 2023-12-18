use crate::signals_logic::{Callable, Patchable, TypeOFSource};
use crate::{
    debug_with_info, dtpserror_context, not_implemented, DataReady, RawData, ResolvedData, ServerStateAccess, DTPSR,
};
use async_trait::async_trait;
use json_patch::Patch;

#[async_trait]
impl Callable for TypeOFSource {
    async fn call(&self, presented_as: &str, ssa: ServerStateAccess, data: &RawData) -> DTPSR<ResolvedData> {
        // debug_with_info!("patching {self:#?} with {patch:#?}");

        not_implemented!("call for {self:#?} with {self:?}")
    }
}
