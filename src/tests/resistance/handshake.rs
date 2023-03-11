use std::{
    net::{IpAddr, Ipv4Addr},
    time::SystemTime,
};

use pea2pea::{
    ConnectionSide,
    ConnectionSide::{Initiator, Responder},
};
use tempfile::TempDir;
use tokio::time::{sleep, Duration};
use ziggurat_core_utils::err_constants::{ERR_NODE_BUILD, ERR_NODE_STOP, ERR_TEMPDIR_NEW};

use crate::{
    protocol::{codecs::message::BinaryMessage, handshake::HandshakeCfg},
    setup::{
        constants::CONNECTION_TIMEOUT,
        node::{ChildExitCode, Node, NodeType},
    },
    tools::{
        config::SynthNodeCfg,
        synth_node::{self, SyntheticNode},
    },
    wait_until,
};

// Empirical values based on some unofficial testing.
const WS_HTTP_HEADER_MAX_SIZE: usize = 7700;
const WS_HTTP_HEADER_INVALID_SIZE: usize = WS_HTTP_HEADER_MAX_SIZE + 300;

// Number of seconds between unix and ripple epoch.
const RIPPLE_EPOCH_OFFSET: u64 = 946684800;

#[allow(non_snake_case)]
#[tokio::test]
async fn r001_t1_HANDSHAKE_reject_if_user_agent_too_long() {
    // ZG-RESISTANCE-001

    // Build and start the Ripple node.
    let target = TempDir::new().expect("couldn't create a temporary directory");
    let mut node = Node::builder()
        .start(target.path(), NodeType::Stateless)
        .await
        .expect("unable to start the node");

    // Start the first synthetic node with a 'User-Agent' header that's too long.
    let mut cfg = SynthNodeCfg::default();
    cfg.handshake = cfg.handshake.map(|mut hs_cfg| {
        hs_cfg.http_ident = format!("{:8192}", 0);
        hs_cfg
    });

    let synth_node1 = SyntheticNode::new(&cfg).await;
    // Ensure this connection was rejected by the node.
    assert!(synth_node1.connect(node.addr()).await.is_err());
    assert_eq!(synth_node1.num_connected(), 0);
    assert!(!synth_node1.is_connected(node.addr()));

    // Start the second synthetic node with the default 'User-Agent'.
    let synth_node2 = SyntheticNode::new(&Default::default()).await;
    synth_node2.connect(node.addr()).await.unwrap();
    // Ensure this connection was successful.
    assert_eq!(synth_node2.num_connected(), 1);
    assert!(synth_node2.is_connected(node.addr()));

    // Shutdown all nodes.
    synth_node1.shut_down().await;
    synth_node2.shut_down().await;
    node.stop().unwrap();
}

#[allow(non_snake_case)]
#[tokio::test]
async fn r001_t2_HANDSHAKE_reject_if_server_too_long() {
    // ZG-RESISTANCE-001

    // Start the first synthetic node. Set identification ('Server' header) for the value that's too long.
    let mut cfg = SynthNodeCfg::default();
    cfg.pea2pea_config.listener_ip = Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)));
    cfg.handshake = cfg.handshake.map(|mut hs_cfg| {
        hs_cfg.http_ident = format!("{:8192}", 0);
        hs_cfg
    });

    let synth_node1 = SyntheticNode::new(&cfg).await;
    let sn1_listening_addr = synth_node1
        .start_listening()
        .await
        .expect("unable to start listening");

    // Start the second synthetic node with the default 'Server' header.
    let mut cfg2 = SynthNodeCfg::default();
    cfg2.pea2pea_config.listener_ip = Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 3)));
    let synth_node2 = SyntheticNode::new(&cfg2).await;
    let sn2_listening_addr = synth_node2
        .start_listening()
        .await
        .expect("unable to start listening");

    // Build and start the Ripple node. Configure its peers such that it connects to the synthetic node above.
    let target = TempDir::new().expect("couldn't create a temporary directory");
    let mut node = Node::builder()
        .initial_peers(vec![sn1_listening_addr, sn2_listening_addr])
        .start(target.path(), NodeType::Stateless)
        .await
        .expect("unable to start the node");

    // Ensure the connection to the second synthetic node was successful.
    wait_until!(CONNECTION_TIMEOUT, synth_node2.num_connected() > 0);

    // Ensure the connection to the first synthetic node was rejected by the node.
    wait_until!(CONNECTION_TIMEOUT, synth_node1.num_connected() == 0);

    // Shutdown all nodes.
    synth_node1.shut_down().await;
    synth_node2.shut_down().await;
    node.stop().unwrap();
}

/// Decide whether to enable node logs and tracing for synthetic nodes.
#[derive(Clone, Copy)]
enum Debug {
    On,
    Off,
}

impl Debug {
    // This API exists just so we could enable the synth tracing once,
    // because calling that function twice would break the test.
    fn enable() -> Self {
        synth_node::enable_tracing();
        Self::On
    }

    fn disable() -> Self {
        // We should use something like synth_node::disable_tracing here (still unimplemented),
        // but we'll never use it anyway so this is good enough
        Self::Off
    }

    /// Convert to a boolean value.
    fn is_on(self) -> bool {
        match self {
            Self::On => true,
            Self::Off => false,
        }
    }
}

// Runs the handshake request test with a given handshake configuration.
// Returns the truthful fact about the relationship with the node.
async fn run_handshake_req_test_with_cfg(cfg: SynthNodeCfg, debug: Debug) -> bool {
    // Spin up a node instance.
    let target = TempDir::new().expect(ERR_TEMPDIR_NEW);
    let mut node = Node::builder()
        .log_to_stdout(debug.is_on())
        .start(target.path(), NodeType::Stateless)
        .await
        .expect(ERR_NODE_BUILD);

    // Create a synthetic node and enable handshaking.
    let mut synthetic_node = SyntheticNode::new(&cfg).await;

    // Connect to the node and initiate the handshake.
    let handshake_established = if synthetic_node.connect(node.addr()).await.is_err() {
        false
    } else {
        // Wait for any message.
        synthetic_node
            .expect_message(&|m: &BinaryMessage| matches!(&m, _))
            .await
    };

    if debug.is_on() {
        // Let us see a few more logs from the node before shutdown.
        sleep(Duration::from_millis(2000)).await;
    }

    // Gracefully shut down the nodes.
    synthetic_node.shut_down().await;
    assert_eq!(node.stop().expect(ERR_NODE_STOP), ChildExitCode::Success);

    handshake_established
}

#[tokio::test]
#[ignore = "internal test"]
async fn normal_handshake() {
    let debug = Debug::enable();

    // Basically, a copy of the C001 test.
    assert!(
        run_handshake_req_test_with_cfg(Default::default(), debug).await,
        "a default configuration doesn't work"
    );
}

/// Generate a string with a given length.
fn gen_huge_string(len: usize) -> String {
    vec!['y'; len].into_iter().collect::<String>()
}

#[allow(non_snake_case)]
#[tokio::test]
async fn r001_t3_HANDSHAKE_connection_field() {
    // ZG-RESISTANCE-001
    // Expected valid value for the "Connection" field in the handshake should be "Upgrade".

    let debug = Debug::disable();

    let gen_cfg = |connection: String| SynthNodeCfg {
        handshake: Some(HandshakeCfg {
            http_connection: connection,
            ..Default::default()
        }),
        ..Default::default()
    };

    // Valid scenarios:

    // These are also valid, but should they be?
    let cfg = gen_cfg("upgrade".to_owned());
    assert!(run_handshake_req_test_with_cfg(cfg, debug).await);
    let cfg = gen_cfg("uPgRAdE".to_owned());
    assert!(run_handshake_req_test_with_cfg(cfg, debug).await);

    // Below tests assert the connection shouldn't be established.

    // Field is almost correct.
    let cfg = gen_cfg("Upgrad".to_owned());
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);
    let cfg = gen_cfg("Upgradee".to_owned());
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);
    let cfg = gen_cfg("UpgradeUpgrade".to_owned());
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);

    // Find the largest instance value which the node could accept, but won't due to invalid value
    // in the field.
    let cfg = gen_cfg(gen_huge_string(WS_HTTP_HEADER_MAX_SIZE));
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);

    // Use a huge value which the node will always reject.
    let cfg = gen_cfg(gen_huge_string(WS_HTTP_HEADER_INVALID_SIZE));
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);

    // Send an empty field.
    let cfg = gen_cfg(String::new());
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);
}

#[allow(non_snake_case)]
#[tokio::test]
async fn r001_t5_HANDSHAKE_crawl_field() {
    // ZG-RESISTANCE-001
    // Expected valid value for the "Crawl" field in the handshake should be "Public" (case insensitive).
    // Other values are considered to be "Private".

    let debug = Debug::enable();

// > User-Agent: rippled-1.9.3+47dec467ea659c1b64c7b5f4eb8a1bfa9759ff91.DEBUG\r\n
// > Upgrade: XRPL/2.0, XRPL/2.1, XRPL/2.2\r\n
// > Connection: Upgrade\r\n
// > Connect-As: Peer\r\n
//
    let gen_cfg = |crawl: String| SynthNodeCfg {
        handshake: Some(HandshakeCfg {
            http_unexpected_extra_field_and_value: Some(crawl),
            ..Default::default()
        }),
        ..Default::default()
    };

    // Valid scenarios:
    //let cfg = gen_cfg("Connect-As: Peer".to_owned());
    //let cfg = gen_cfg("".to_owned());
    let append_str = |h: &str, v: &str| -> String {
        format!("{h}:{v}/r/n")
    };

    let mut field = String::new();
    for letter in 'a'..='z' {
        for i in 0..10 {
            field += &append_str(letter.to_string().as_str(), i.to_string().as_str());
        }
    }
    for letter in 'A'..='Z' {
        for i in 0..10 {
            field += &append_str(letter.to_string().as_str(), i.to_string().as_str());
        }
    }
    for letter in '0'..='9' {
        for i in 'a'..='z' {
            field += &append_str(letter.to_string().as_str(), i.to_string().as_str());
        }
    }
    for letter in '0'..='9' {
        for i in 'A'..='Z' {
            field += &append_str(letter.to_string().as_str(), i.to_string().as_str());
        }
    }

    let cfg = gen_cfg(format!("{field}b:1"));
    assert!(run_handshake_req_test_with_cfg(cfg, debug).await);

    // This is also valid, but should it be? The node checks for "public" to determine whether it's public,
    // everything else is considered not public.

    // Invalid scenarios:

    // Use a huge value that the node will always reject.
}

#[allow(non_snake_case)]
#[tokio::test]
async fn r001_t4_HANDSHAKE_connect_as_field() {
    // ZG-RESISTANCE-001
    // Expected valid value for the "Connect-As" field in the handshake should be "Peer".

    let debug = Debug::disable();

    let gen_cfg = |connect_as: String| SynthNodeCfg {
        handshake: Some(HandshakeCfg {
            http_connect_as: connect_as,
            ..Default::default()
        }),
        ..Default::default()
    };

    // Valid scenarios:

    // These are also valid, but should they be?
    let cfg = gen_cfg("peer".to_owned());
    assert!(run_handshake_req_test_with_cfg(cfg, debug).await);
    let cfg = gen_cfg("PeER".to_owned());
    assert!(run_handshake_req_test_with_cfg(cfg, debug).await);

    // Below tests assert the connection shouldn't be established.

    // Field is almost correct.
    let cfg = gen_cfg("Pee".to_owned());
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);
    let cfg = gen_cfg("Peerr".to_owned());
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);
    let cfg = gen_cfg("PeerPeer".to_owned());
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);

    // Find the largest instance value that the node could accept, but won't due to invalid value
    // in the field.
    let cfg = gen_cfg(gen_huge_string(WS_HTTP_HEADER_MAX_SIZE));
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);

    // Use a huge value that the node will always reject.
    let cfg = gen_cfg(gen_huge_string(WS_HTTP_HEADER_INVALID_SIZE));
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);

    // Send an empty field.
    let cfg = gen_cfg(String::new());
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);
}

#[allow(non_snake_case)]
#[tokio::test]
async fn r001_t6_HANDSHAKE_x_protocol_ctl_field() {
    // ZG-RESISTANCE-001
    // Expected valid value for the "X-Protocol-Ctl" field in the handshake should be a valid string.

    let debug = Debug::disable();

    let gen_cfg = |protocol: String| SynthNodeCfg {
        handshake: Some(HandshakeCfg {
            http_x_protocol_ctl: protocol,
            ..Default::default()
        }),
        ..Default::default()
    };

    // These are also valid, but should they be?
    let cfg = gen_cfg("leDgErrEpLay=하나;TXRR=да;".to_owned());
    assert!(run_handshake_req_test_with_cfg(cfg, debug).await);

    let cfg = gen_cfg("leDgErrEpLay=I'm happy to accept anything really;".to_owned());
    assert!(run_handshake_req_test_with_cfg(cfg, debug).await);

    let cfg = gen_cfg("unknown_option_here_is_fine=One11111!!;".to_owned());
    assert!(run_handshake_req_test_with_cfg(cfg, debug).await);

    // Find the largest instance value which the node could accept.
    let cfg = gen_cfg(gen_huge_string(WS_HTTP_HEADER_MAX_SIZE));
    assert!(run_handshake_req_test_with_cfg(cfg, debug).await);

    // Send an empty field.
    let cfg = gen_cfg(String::new());
    assert!(run_handshake_req_test_with_cfg(cfg, debug).await);

    // Use a huge value which the node will always reject.
    let cfg = gen_cfg(gen_huge_string(WS_HTTP_HEADER_INVALID_SIZE));
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);
}

#[allow(non_snake_case)]
#[tokio::test]
async fn r001_t7_HANDSHAKE_network_time_field() {
    // ZG-RESISTANCE-001
    // Expected valid value for the "Network-Time" field in the handshake should be a valid number.

    let debug = Debug::disable();

    let gen_cfg = |time: String| SynthNodeCfg {
        handshake: Some(HandshakeCfg {
            http_network_time: Some(time),
            ..Default::default()
        }),
        ..Default::default()
    };

    let time_now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - RIPPLE_EPOCH_OFFSET;
    // Valid value for Network-Time
    let cfg = gen_cfg(format!("{time_now}"));
    assert!(run_handshake_req_test_with_cfg(cfg, debug).await);

    // Find the largest instance value which the node could accept.
    // Send correct value but prepend lots of zeros.
    let cfg = gen_cfg(format!("{:#07000}", time_now));
    assert!(run_handshake_req_test_with_cfg(cfg, debug).await);

    // Invalid values for Network-Time
    let cfg = gen_cfg(format!("{}", time_now - 24 * 60 * 60));
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);

    let cfg = gen_cfg(format!("{}", time_now + 24 * 60 * 60));
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);

    let cfg = gen_cfg(format!("{time_now}xyz"));
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);

    // Send an empty field.
    let cfg = gen_cfg(String::new());
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);

    // Use a huge value which the node will always reject.
    // Send correct value but prepend too many zeros.
    let cfg = gen_cfg(format!("{:#08000}", time_now));
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);
}

#[allow(non_snake_case)]
#[tokio::test]
async fn r001_t8_HANDSHAKE_upgrade_req_field() {
    // ZG-RESISTANCE-001
    // Expected valid value for the "Upgrade" field in the handshake should be valid.

    let debug = Debug::disable();

    let gen_cfg = |version: String| SynthNodeCfg {
        handshake: Some(HandshakeCfg {
            http_upgrade_req: version,
            ..Default::default()
        }),
        ..Default::default()
    };

    // Valid value for Upgrade
    let cfg = gen_cfg("XRPL/2.2".to_owned());
    assert!(run_handshake_req_test_with_cfg(cfg, debug).await);

    // Find the largest instance value which the node could accept.
    let cfg = gen_cfg(format!("{:#7700}", "XRPL/2.2"));
    assert!(run_handshake_req_test_with_cfg(cfg, debug).await);

    // Invalid values
    // Large version.
    let cfg = gen_cfg("XRPL/20.2".to_owned());
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);

    // Negative version number.
    let cfg = gen_cfg("XRPL/-2.2".to_owned());
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);

    // Send empty version.
    let cfg = gen_cfg("XRPL/".to_owned());
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);

    // Send an empty field.
    let cfg = gen_cfg(String::new());
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);

    // Use a huge value which the node will always reject.
    let cfg = gen_cfg(format!("{:#8000}", "XRPL/2.2"));
    assert!(!run_handshake_req_test_with_cfg(cfg, debug).await);
}

#[allow(non_snake_case)]
#[tokio::test]
async fn r003_t1_HANDSHAKE_reject_if_public_key_has_bit_flipped() {
    // ZG-RESISTANCE-003

    // Prepare config for a synthetic node. Flip bit in the public_key.
    let mut cfg = SynthNodeCfg::default();
    cfg.handshake = cfg.handshake.map(|mut hs_cfg| {
        hs_cfg.bitflip_pub_key = true;
        hs_cfg
    });

    run_and_assert_handshake_failure(&cfg, Responder).await;
    run_and_assert_handshake_failure(&cfg, Initiator).await;
}

#[allow(non_snake_case)]
#[tokio::test]
async fn r003_t2_HANDSHAKE_reject_if_shared_value_has_bit_flipped() {
    // ZG-RESISTANCE-003

    // Prepare config for a synthetic node. Flip bit in the shared_value.
    let mut cfg = SynthNodeCfg::default();
    cfg.handshake = cfg.handshake.map(|mut hs_cfg| {
        hs_cfg.bitflip_shared_val = true;
        hs_cfg
    });

    run_and_assert_handshake_failure(&cfg, Responder).await;
    run_and_assert_handshake_failure(&cfg, Initiator).await;
}

async fn run_and_assert_handshake_failure(config: &SynthNodeCfg, connection_side: ConnectionSide) {
    // Start a SyntheticNode with the required config.
    let synth_node = SyntheticNode::new(config).await;
    let listening_addr = synth_node
        .start_listening()
        .await
        .expect("unable to start listening");

    // Build and start the Ripple node.
    let target = TempDir::new().expect("couldn't create a temporary directory");
    let initial_peers = match connection_side {
        Initiator => vec![],
        Responder => vec![listening_addr],
    };
    let mut node = Node::builder()
        .initial_peers(initial_peers)
        .start(target.path(), NodeType::Stateless)
        .await
        .expect("unable to start the node");

    // Try to connect to rippled if Initiator side.
    if connection_side == Initiator {
        assert!(synth_node.connect(node.addr()).await.is_ok());
    }
    // Sleep for some time. This is needed either for:
    // 1. Rippled to connect to the synth node (for Responder side) and reject the handshake,
    // 2. Rippled to drop connection after an unsuccessful handshake (for Initiator side)
    wait_until!(
        CONNECTION_TIMEOUT,
        !synth_node.is_connected_ip(node.addr().ip())
    );

    // Shutdown all nodes.
    synth_node.shut_down().await;
    node.stop().unwrap();
}
