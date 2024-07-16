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
const MIN_DISTANCE_SCALE: f32 = 1.5_f32;
const MAX_DISTANCE_SCALE: f32 = 0.75_f32;
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
enum AlertKind {AlertMin = 0, DangerMin = 1, AlertMax = 2, DangerMax = 3}
#[derive (Serialize, Deserialize, Debug)]
struct DistanceAlert {
    pub ida: String,
    pub idb: String,
    pub distance: f32,
    pub kind: AlertKind
}

#[tokio::main]
async fn main() {
    let (skey,
        pkey,
        min_distance,
        max_distance,
        compute_period_ms,
        config) = parse_args();

    let z = Arc::new(zenoh::open(config).res().await.unwrap());
    let zt = z.clone();
    let sub = z.declare_subscriber(skey).res().await.unwrap();
    let pmap = Arc::new(Mutex::new(Box::new(HashMap::<String, VehicleInfo>::new())));
    let pmapc = pmap.clone();
    task::spawn(async move {
        loop {
            let mut map =  {
                let mut m = pmapc.lock().await;
                let emap = Box::new(HashMap::<String, VehicleInfo>::new());
                std::mem::replace(&mut *m, emap)
            };
            let mut n = 0_usize;
            for (cid, cv) in map.iter() {
                n += 1;
                for (oid, ov) in map.iter().skip(n) {
                    let distance = cv.position.distance_haverside(&ov.position);
                    if cid != oid {
                        if distance <= min_distance {
                            println!("DANGER: {cid} -> {oid} = {distance} <? {min_distance}");
                            let da = DistanceAlert { ida: cid.clone(), idb: oid.clone(), distance, kind: AlertKind::DangerMin };
                            let bs = serde_json::to_vec(&da).unwrap();
                            zt.put(&pkey, bs).encoding(Encoding::APP_JSON).res().await.unwrap()
                        } else if  distance <= (min_distance * MIN_DISTANCE_SCALE)  {
                            println!("ALERT: {cid} -> {oid} = {distance} <? {min_distance}");
                            let da = DistanceAlert { ida: cid.clone(), idb: oid.clone(), distance, kind: AlertKind::AlertMin };
                            let bs = serde_json::to_vec(&da).unwrap();
                            zt.put(&pkey, bs).encoding(Encoding::APP_JSON).res().await.unwrap()
                        }
                        if distance > max_distance {
                            println!("DANGER: {cid} -> {oid} = {distance} <? {max_distance}");
                            let da = DistanceAlert { ida: cid.clone(), idb: oid.clone(), distance, kind: AlertKind::DangerMin };
                            let bs = serde_json::to_vec(&da).unwrap();
                            zt.put(&pkey, bs).encoding(Encoding::APP_JSON).res().await.unwrap()
                        } else if  distance > (max_distance * MAX_DISTANCE_SCALE)  {
                            println!("ALERT: {cid} -> {oid} = {distance} <? {max_distance}");
                            let da = DistanceAlert { ida: cid.clone(), idb: oid.clone(), distance, kind: AlertKind::DangerMax };
                            let bs = serde_json::to_vec(&da).unwrap();
                            zt.put(&pkey, bs).encoding(Encoding::APP_JSON).res().await.unwrap()
                        } else {
                            println!("INFO: {cid} -> {oid} = {distance}");
                        }
                    }
                }
            }
            {
                let mut cmap = pmapc.lock().await;
                for (id, vi) in cmap.iter() {
                    map.insert(id.clone(), vi.clone());
                }
                let _ = std::mem::replace(&mut *cmap, map);
            }
            let _ = tokio::time::sleep(Duration::from_millis(compute_period_ms)).await;
        }
    });
    while let Ok(sample) = sub.recv_async().await {
        let payload = sample.payload.contiguous();
        match serde_json::from_slice::<VehicleInfo>(payload.as_ref()) {
            Ok(vi) => {
                let mut map = pmap.lock().await;
                println!("Received: {:?}", &vi);
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
    min_distance: Option<f32>,
    #[arg(long)]
    max_distance: Option<f32>,
    #[arg(long)]
    compute_period_ms: Option<u64>,
    #[arg(long)]
    config: Option<String>
}

fn parse_args() -> (String, String, f32, f32, u64, Config) {
    let args = AppArgs::parse();

    let skey = args.sub_key.unwrap_or("demo/tracker/mobs/**".into());
    let min_distance = args.min_distance.unwrap_or(10.0_f32);
    let max_distance = args.max_distance.unwrap_or(1000_f32);
    let pkey = args.pub_key.unwrap_or("demo/tracker/alert/distance".into());
    let compute_period_ms = args.compute_period_ms.unwrap_or(500);
    let config = match args.config {
        Some(f) => Config::from_file(f).unwrap(),
        None => Config::default()
    };

    (skey, pkey, min_distance, max_distance, compute_period_ms, config)

}
