//! Which MQTT 5 connect options does a broker accept? usage: cargo run --example connect_check -- host port
use std::time::Duration;

use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::LastWill;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};

async fn try_connect(name: &str, host: &str, port: u16, f: impl FnOnce(&mut MqttOptions)) {
    let mut o = MqttOptions::new(format!("check-{name}"), host, port);
    f(&mut o);
    let (_c, mut el) = AsyncClient::new(o, 10);
    let r = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match el.poll().await {
                Ok(Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::ConnAck(a))) => return format!("ok ({:?})", a.code),
                Ok(_) => {}
                Err(e) => return format!("error: {e}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| "timeout".into());
    println!("{name:24} {r}");
}

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let host = a.get(1).cloned().unwrap_or("127.0.0.1".into());
    let port: u16 = a.get(2).and_then(|p| p.parse().ok()).unwrap_or(1883);
    try_connect("defaults", &host, port, |_| {}).await;
    try_connect("clean_start", &host, port, |o| { o.set_clean_start(true); }).await;
    try_connect("max_packet_size", &host, port, |o| { o.set_max_packet_size(Some(64 * 1024 * 1024)); }).await;
    try_connect("last_will", &host, port, |o| {
        o.set_last_will(LastWill::new("octopus/member/x", "", QoS::AtMostOnce, true, None));
    }).await;
    try_connect("will_not_retained", &host, port, |o| {
        o.set_last_will(LastWill::new("octopus/member/x", "", QoS::AtMostOnce, false, None));
    }).await;
    try_connect("will_payload", &host, port, |o| {
        o.set_last_will(LastWill::new("octopus/member/x", "gone", QoS::AtMostOnce, true, None));
    }).await;
    try_connect("will_payload_qos1", &host, port, |o| {
        o.set_last_will(LastWill::new("octopus/member/x", "gone", QoS::AtLeastOnce, true, None));
    }).await;
    try_connect("all", &host, port, |o| {
        o.set_clean_start(true);
        o.set_max_packet_size(Some(64 * 1024 * 1024));
        o.set_last_will(LastWill::new("octopus/member/x", "", QoS::AtMostOnce, true, None));
    }).await;
}
