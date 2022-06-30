use std::env;

fn print_usage(cmd: &str) {
    eprintln!("Usage: {} config f id replen #crypto_threads", cmd);
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
    let replen = args[4].parse::<usize>().unwrap();
    let crypto_threads = args[5].parse::<usize>().unwrap();
    let smr = rusty_bft::statemachine::Replica::new(&args[1], f, id, crypto_threads);

    println!(
        "Hello, world! I'm replica {}/{}: {:?}",
        smr.id,
        smr.n,
        smr.my_address()
    );

    smr.run_replica(&|_o: Vec<u8>| -> Vec<u8> {
        //println!("Replica {} has received operation {:?} to execute", id, _o);
        vec![0; replen]
    });
}
