#[cfg(test)]
pub mod tests {
    use std::time::Duration;

    use maplit::hashmap;
    use rstest::rstest;

    use crate::client_proxy::add_proxy;
    use crate::client_tpt::{add_tpt_connection, remove_tpt_connection};
    use crate::client_verbs::post_json;
    use crate::connections::TypeOfConnection;
    use crate::error_other;
    use crate::{
        debug_with_info, dtpserror_other, init_logging, make_request2,
        test_fixtures::TestFixture,
        test_range::tests::{instance, node1, node2, switchboard},
        ConnectionJob, ResponseResult, ResponseUnobtained, ServiceMode, TopicName, TopicProperties, CONTENT_TYPE_CBOR,
        DTPSR,
    };
    // #[rstest]
    // #[tokio::test]
    // async fn unreachable_by_ip() -> DTPSR<()> {
    //     let url = "http://10.193.134.13";
    //     let con = TypeOfConnection::from_string(url)?;
    //     let r = make_request2(&con, hyper::Method::GET, b"",
    //                           None, None).await?;
    //     debug_with_info!("r: {:#?}", r);
    //
    //     // instance.finish().await
    //
    //     Ok(())
    // }

    #[rstest]
    #[tokio::test]
    async fn unreachable_by_dns() -> DTPSR<()> {
        init_logging();
        let url = "http://this.domain.does.not.exist";
        let con = TypeOfConnection::from_string(url)?;
        let r = make_request2(&con, hyper::Method::GET, b"", None, None).await?;
        debug_with_info!("r: {:#?}", r);

        match r {
            ResponseResult::ResponseObtained(_r) => {
                return dtpserror_other!("Should not have been able to connect");
            }
            ResponseResult::ResponseUnobtained(ru) => match ru {
                ResponseUnobtained::DNSError(_) => {}
                _ => {
                    return dtpserror_other!("Expected a DNS error");
                }
            },
        }

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn unreachable_by_refused() -> DTPSR<()> {
        init_logging();

        let url = "http://127.0.0.1:63999";
        let con = TypeOfConnection::from_string(url)?;
        let r = make_request2(&con, hyper::Method::GET, b"", None, None).await?;
        debug_with_info!("r: {:#?}", r);

        match r {
            ResponseResult::ResponseObtained(_r) => {
                return dtpserror_other!("Should not have been able to connect");
            }
            ResponseResult::ResponseUnobtained(ru) => match ru {
                ResponseUnobtained::ConnectionRefused(_) => {}
                _ => {
                    return dtpserror_other!("expected ConnectionRefused");
                }
            },
        }

        Ok(())
    }
}
