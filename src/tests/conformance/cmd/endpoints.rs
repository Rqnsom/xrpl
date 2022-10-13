use crate::{
    protocol::codecs::binary::{BinaryMessage, Payload},
    tests::conformance::perform_response_test,
    tools::config::TestConfig,
};

#[should_panic]
#[tokio::test]
#[allow(non_snake_case)]
async fn c020_TM_ENDPOINTS_node_should_not_send_endpoints_if_no_handshake() {
    // ZG-CONFORMANCE-020

    // Check for a TmEndpoints message.
    let response_check = |m: &BinaryMessage| matches!(&m.payload, Payload::TmEndpoints(..));
    perform_response_test(TestConfig::default().with_handshake(false), &response_check).await;
}
