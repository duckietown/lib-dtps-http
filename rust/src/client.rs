use std::fmt::Debug;

use hex;
use hyper::{self};

//
// #[macro_export]
// macro_rules! loop_broadcast_receiver {
//     ( ($rx: ident, $value: ident) $body: block) =>  {
//         while let Some($value) = $crate::wrap_recv(&mut $rx).await { $body }
//     }
// }
