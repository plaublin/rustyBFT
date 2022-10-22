use rand::Rng;
use std::env;
use std::fs::File;
use std::io::Write;
use std::sync::mpsc;
use std::{thread, time};

fn print_usage(cmd: &str) {
    eprintln!(
        "Usage: {} config f id nclients duration_seq get_ratio #kvs_entries save_latencies",
        cmd
    );
}

fn main() {
    // read command line
    let args: Vec<String> = env::args().collect();
    if args.len() < 10 {
        print_usage(&args[0]);
        return;
    }

    // read config file
    let f = args[2].parse::<u32>().unwrap();
    let id = args[3].parse::<u32>().unwrap();
    let nc = args[4].parse::<u32>().unwrap();
    let duration = time::Duration::from_millis(args[5].parse::<u64>().unwrap() * 1000);
    let get_ratio = args[6].parse::<f32>().unwrap();
    let kvs_entries = args[7].parse::<usize>().unwrap();
    let save_latencies = args[8].parse::<u32>().unwrap() == 1;

    let mut handles = Vec::new();
    let (tx, rx) = mpsc::channel();
    for i in 0..nc {
        let config = args[1].clone();
        let my_tx = tx.clone();
        let h = thread::spawn(move || {
            let mut latencies = Vec::with_capacity(if save_latencies { 50000 } else { 0 });
            let smr = rusty_bft::statemachine::Client::new(&config, f, id + i);

            println!(
                "Hello, world! I'm KVS client {}: {:?}",
                smr.id,
                smr.my_address()
            );

            let mut accepted: u64 = 0;
            let mut n_get: u64 = 0;
            let mut sum_lat: u128 = 0;

            let start = time::Instant::now();
            while start.elapsed() < duration {
                // create request
                // is it a get or set request?
                let n_get_should_sent = (accepted as f32) * get_ratio;
                let get = (n_get as f32) < n_get_should_sent;

                let c = rusty_bft::kvs::KVSCommand {
                    mode: if get {
                        rusty_bft::kvs::KVSRequest::Get
                    } else {
                        rusty_bft::kvs::KVSRequest::Set
                    },
                    key: rand::thread_rng().gen_range(0..kvs_entries),
                    value: if get {
                        None
                    } else {
                        Some([0; rusty_bft::kvs::KVS_VALUE_LEN].to_vec())
                    },
                };

                println!("Client {} operation is {:?}", smr.id, c);
                if get {
                    n_get += 1;
                    println!(
                        "Client {} send get request: {} / {}, ratio {}",
                        smr.id,
                        n_get,
                        accepted,
                        (n_get as f32) / (accepted as f32)
                    );
                } else {
                    println!(
                        "Client {} send rw request: {} / {}",
                        smr.id, n_get, accepted
                    );
                }

                let o = rusty_bft::kvs::create_request(c);
                let mut req = smr.create_request(get, o.len());
                let _ = req
                    .message_payload_mut::<rusty_bft::message::Request>()
                    .unwrap()
                    .write(&o);

                let lat_start = time::Instant::now();
                let rep = smr.invoke(&req);
                let lat = lat_start.elapsed().as_micros();

                let payload = rep.message_payload::<rusty_bft::message::Reply>().unwrap();
                let o = rusty_bft::kvs::parse_request(payload);
                println!("Client {} received reply from KVS: {:?}", smr.id, o);

                accepted += 1;
                sum_lat += lat;
                if save_latencies {
                    latencies.push((get, lat));
                }

                if accepted % 10000 == 0 {
                    println!("Client {} accepted {}", smr.id, accepted);
                }

                std::thread::sleep(time::Duration::from_millis(1000));
            }

            let duration = start.elapsed().as_secs_f64();
            let thr = accepted as f64 / duration;
            let avg_lat = if accepted > 0 {
                sum_lat / (accepted as u128)
            } else {
                0
            };

            println!(
                "Client {} has finished after {} sec with {} accepted requests, thr = {} ops/s, lat = {} usec",
                smr.id,
                duration,
                accepted,
                thr,
                avg_lat,
            );

            my_tx.send((i, thr, avg_lat, latencies)).unwrap();
        });
        handles.push(h);
    }

    let mut file = File::create("latencies.log").expect("Unable to create latencies file");
    let mut global_thr = 0.0;
    let mut global_lat = 0;
    for h in handles {
        let (tid, thr, lat, latencies) = rx.recv().unwrap();
        if thr > 0.0 && lat > 0 {
            global_thr += thr;
            global_lat += lat;
        }

        if save_latencies && !latencies.is_empty() {
            for (i, l) in latencies.iter().enumerate() {
                writeln!(file, "{} {} {} {}", tid, i, l.0, l.1).unwrap();
            }
        }

        let _ = h.join();
    }

    println!(
        "Global stats: thr = {} ops/s, lat = {} usec",
        global_thr,
        global_lat / nc as u128
    );
}
