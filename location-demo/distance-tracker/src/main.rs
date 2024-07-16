use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use zenoh::prelude::r#async::*;
use serde::{Serialize, Deserialize};
use tokio::sync::Mutex;
use tokio::task;
use clap::Parser;

// Linear Deformation factor

const EARTH_RADIUS: f32 = 6371.0;
#[derive (Serialize, Deserialize, Debug, Clone, Copy)]
struct Position {
    pub lat: f32,
    pub lng: f32
}

impl Position {
    pub fn distance_haverside(&self, other: &Position) -> f32 {
        let c_lat = self.lat.to_radians();
        let o_lat = other.lat.to_radians();

        let delta_lat = (other.lat - self.lat).to_radians();
        let delta_lng = (other.lng - self.lng).to_radians();

        let central_angle_inner = (delta_lat / 2.0).sin().powi(2)
            + c_lat.cos() * o_lat.cos() * (delta_lng / 2.0).sin().powi(2);

        let central_angle = 2.0 * central_angle_inner.sqrt().asin();
        EARTH_RADIUS * central_angle * 1000.0 // distance in meters
    }
}
#[derive (Serialize, Deserialize, Debug, Clone)]
struct VehicleInfo {
    pub position: Position,
    pub speed: f32,
    pub color: String,
    pub id: String,
    pub kind: String
}

#[derive (Serialize, Deserialize, Debug)]
enum AlertKind {ALERT = 0, DANGER = 1}
#[derive (Serialize, Deserialize, Debug)]
struct DistanceAlert {
    pub ida: String,
    pub idb: String,
    pub distance: f32,
    pub kind: AlertKind
}

#[tokio::main]
async fn main() {
    let (skey, pkey, alert_distance, config) = parse_args();
    let z = Arc::new(zenoh::open(config).res().await.unwrap());
    let zt = z.clone();
    let sub = z.declare_subscriber(skey).res().await.unwrap();
    let pmap = Arc::new(Mutex::new(Box::new(HashMap::<String, VehicleInfo>::new())));
    let pmap2 = pmap.clone();
    task::spawn(async move {
        loop {
            let mut map =  {
                let mut m = pmap2.lock().await;
                let emap = Box::new(HashMap::<String, VehicleInfo>::new());
                std::mem::replace(&mut *m, emap)
            };
            for (cid, cv) in map.iter() {
                for (oid, ov) in map.iter() {
                    let distance = cv.position.distance_haverside(&ov.position);
                    if cid != oid {
                        println!("{cid} -> {oid} = {distance} <? {alert_distance}");
                        if distance <= alert_distance {
                            println!("DANGER: {cid} -> {oid} = {distance} <? {alert_distance}");
                            let da = DistanceAlert { ida: cid.clone(), idb: oid.clone(), distance, kind: AlertKind::DANGER };
                            let bs = serde_json::to_vec(&da).unwrap();
                            zt.put(&pkey, bs).encoding(Encoding::APP_JSON).res().await.unwrap()
                        } else if  distance <= (alert_distance * 1.5_f32)  {
                            println!("ALERT: {cid} -> {oid} = {distance} <? {alert_distance}");
                            let da = DistanceAlert { ida: cid.clone(), idb: oid.clone(), distance, kind: AlertKind::ALERT };
                            let bs = serde_json::to_vec(&da).unwrap();
                            zt.put(&pkey, bs).encoding(Encoding::APP_JSON).res().await.unwrap()
                        } else {
                            println!("INFO: {cid} -> {oid} = {distance} <? {alert_distance}");
                        }
                    }
                }
            }
            {
                let mut cmap = pmap2.lock().await;
                for (id, vi) in cmap.iter() {
                    map.insert(id.clone(), vi.clone());
                }
                let _ = std::mem::replace(&mut *cmap, map);
            }
            let _ = tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
    while let Ok(sample) = sub.recv_async().await {
        let payload = sample.payload.contiguous();
        println!("Received: {}", String::from_utf8_lossy(&payload));
        match serde_json::from_slice::<VehicleInfo>(payload.as_ref()) {
            Ok(vi) => {
                let mut map = pmap.lock().await;
                map.insert(vi.id.clone(), vi);
            },
            Err(e) => {
                println!("Unable to Deserialize:\n ${e}");
            }
        }


    }
}

#[derive(clap_derive::Parser)]

struct AppArgs {
    #[arg(long)]
    sub_key: Option<String>,
    #[arg(long)]
    pub_key: Option<String>,
    #[arg(long)]
    distance: Option<f32>,
    #[arg(long)]
    config: Option<String>
}

fn parse_args() -> (String, String, f32, Config) {
    let args = AppArgs::parse();

    let skey = args.sub_key.unwrap_or("demo/tracker/mobs/**".into());
    let distance = args.distance.unwrap_or(2.0_f32);
    let pkey = args.pub_key.unwrap_or("demo/tracker/alert/distance".into());

    let config = match args.config {
        Some(f) => Config::from_file(f).unwrap(),
        None => Config::default()
    };

    (skey, pkey, distance, config)

}
