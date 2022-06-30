use std::env;
use std::sync::mpsc;
use std::{thread, time};

fn print_usage(cmd: &str) {
    eprintln!("Usage: {} config f id nclients duration_seq reqlen", cmd);
}

fn main() {
    // read command line
    let args: Vec<String> = env::args().collect();
    if args.len() < 6 {
        print_usage(&args[0]);
        return;
    }

    // read config file
    let f = args[2].parse::<u32>().unwrap();
    let id = args[3].parse::<u32>().unwrap();
    let nc = args[4].parse::<u32>().unwrap();
    let duration = time::Duration::from_millis(args[5].parse::<u64>().unwrap() * 1000);
    let reqlen = args[6].parse::<usize>().unwrap();

    let mut handles = Vec::new();
    let (tx, rx) = mpsc::channel();
    for i in 0..nc {
        let config = args[1].clone();
        let my_tx = tx.clone();
        let h = thread::spawn(move || {
            let smr = rusty_bft::statemachine::Client::new(&config, f, id + i);

            println!(
                "Hello, world! I'm client {}: {:?}",
                smr.id,
                smr.my_address()
            );

            let mut accepted: u64 = 0;
            let mut sum_lat: u128 = 0;

            let start = time::Instant::now();
            while start.elapsed() < duration {
                // create request
                let req = smr.create_request(reqlen);

                let lat_start = time::Instant::now();
                let _ = smr.invoke(&req);
                let lat = lat_start.elapsed().as_micros();

                accepted += 1;
                sum_lat += lat;

                if accepted % 10000 == 0 {
                    println!("Client {} accepted {}", smr.id, accepted);
                }

                // std::thread::sleep(time::Duration::from_millis(1000));
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

            my_tx.send((thr, avg_lat)).unwrap();
        });
        handles.push(h);
    }

    let mut global_thr = 0.0;
    let mut global_lat = 0;
    for h in handles {
        let (thr, lat) = rx.recv().unwrap();
        global_thr += thr;
        global_lat += lat;

        let _ = h.join();
    }

    println!(
        "Global stats: thr = {} ops/s, lat = {} usec",
        global_thr,
        global_lat / (nc as u128),
    );
}
