//! The brokerless ZeroMQ fabric on loopback: two dispatchers and a client.

use std::time::Duration;

use bytes::Bytes;
use octopus_fabric::{Fabric, Incoming, ZmqConfig, ZmqPeer};
use tokio::sync::mpsc::Receiver;

async fn next(rx: &mut Receiver<Incoming>) -> Incoming {
    tokio::time::timeout(Duration::from_secs(10), rx.recv()).await.expect("a message within 10 s").expect("open")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routes_requests_replies_announcements_and_peer_messages() {
    let peers = ZmqPeer::parse_list("d1@127.0.0.1:47101,d2@127.0.0.1:47103").unwrap();
    let mut cfg = ZmqConfig::new("d1", peers.clone());
    cfg.bind_host = "127.0.0.1".into();
    let (d1, mut d1_rx) = Fabric::connect_zmq(cfg).await.unwrap();
    let mut cfg = ZmqConfig::new("d2", peers.clone());
    cfg.bind_host = "127.0.0.1".into();
    let (d2, mut d2_rx) = Fabric::connect_zmq(cfg).await.unwrap();
    let (c, mut c_rx) = Fabric::connect_zmq(ZmqConfig::new("c1", peers)).await.unwrap();
    c.subscribe("ns/member/+").await.unwrap();
    c.subscribe("ns/inbox/c1").await.unwrap();
    d1.subscribe("$share/dispatchers/ns/pub/+").await.unwrap();

    // connections come up in the background
    for _ in 0..100 {
        if c.zmq_connected().len() == 2 && d1.zmq_connected() == ["d2"] {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(c.zmq_connected().len(), 2);

    // announcements reach subscribers and mark the dispatchers as live
    d1.publish_retained("ns/member/d1", Bytes::from_static(b"m1")).await.unwrap();
    d2.publish_retained("ns/member/d2", Bytes::from_static(b"m2")).await.unwrap();
    let mut got = vec![next(&mut c_rx).await.topic, next(&mut c_rx).await.topic];
    got.sort();
    assert_eq!(got, ["ns/member/d1", "ns/member/d2"]);

    // a shared publish goes to exactly one dispatcher (round robin): 4 publishes, 2 each
    for i in 0..4 {
        c.publish(format!("ns/pub/g{i}"), Bytes::from(vec![i as u8; 100_000])).await.unwrap();
    }
    let a = next(&mut d1_rx).await;
    let b = next(&mut d1_rx).await;
    let e = next(&mut d2_rx).await;
    let f = next(&mut d2_rx).await;
    for m in [&a, &b, &e, &f] {
        assert!(m.topic.starts_with("ns/pub/g") && m.payload.len() == 100_000);
    }

    // a reply to the client's inbox, through the dispatcher's ROUTER
    d2.publish("ns/inbox/c1", Bytes::from_static(b"ack")).await.unwrap();
    let r = next(&mut c_rx).await;
    assert_eq!((r.topic.as_str(), &r.payload[..]), ("ns/inbox/c1", &b"ack"[..]));

    // dispatcher to dispatcher, and to oneself
    d1.publish("ns/inbox/d2", Bytes::from_static(b"hint")).await.unwrap();
    assert_eq!(&next(&mut d2_rx).await.payload[..], b"hint");
    d1.publish("ns/inbox/d1", Bytes::from_static(b"self")).await.unwrap();
    assert_eq!(&next(&mut d1_rx).await.payload[..], b"self");

    // a client cannot announce
    assert!(c.publish_retained("ns/member/c1", Bytes::new()).await.is_err());
}
