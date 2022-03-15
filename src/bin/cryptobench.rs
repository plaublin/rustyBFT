use std::time;

const NONE_KEY: &str = "NONE";
const HMAC_KEY: &str = "replica1";
//                                              pub key ------------------------->|<-- priv key
const KEY: &str = "42177206f5e9a64b12f44826bf917a65e958aaf2cd97464be33e8f7d86a65d722b794e21f3fd0ac8bdef1172f4f6cb1405043e469d33b812342a8a8f41b882c5";
const MESSAGE_LEN: usize = 4096;

fn hmac_bench() {
    let crypto = rusty_bft::Cryptografer::new(HMAC_KEY.to_string(), NONE_KEY.to_string());
    let m = [0u8; MESSAGE_LEN];

    let start = time::Instant::now();
    let round = 10000000;
    for _ in 0..round {
        let _ = crypto.gen_hmac(&m);
    }
    let elapsed = start.elapsed().as_secs_f64();

    let speed = (round as f64) / elapsed;
    println!("HMAC speed: {:.0} per second", speed);
}

fn sign_bench() {
    let crypto = rusty_bft::Cryptografer::new(NONE_KEY.to_string(), KEY.to_string());
    let m = [0u8; MESSAGE_LEN];

    let start = time::Instant::now();
    let round = 500000;
    for _ in 0..round {
        let _ = crypto.gen_sign(&m);
    }
    let elapsed = start.elapsed().as_secs_f64();

    let speed = (round as f64) / elapsed;
    println!("Sign speed: {:.0} per second", speed);
}

fn verify_bench() {
    let crypto = rusty_bft::Cryptografer::new(NONE_KEY.to_string(), KEY.to_string());
    let m = [0u8; MESSAGE_LEN];

    let s = crypto.gen_sign(&m);

    let start = time::Instant::now();
    let round = 100000;
    for _ in 0..round {
        let _ = crypto.verify_sign(&m, &s);
    }
    let elapsed = start.elapsed().as_secs_f64();

    let speed = (round as f64) / elapsed;
    println!("Verify speed: {:.0} per second", speed);
}

fn main() {
    println!("Benchmarking with message size {} bytes", MESSAGE_LEN);

    hmac_bench();
    sign_bench();
    verify_bench();
}
