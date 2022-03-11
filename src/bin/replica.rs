use std::env;

fn print_usage(cmd: &str) {
    eprintln!("Usage: {} config f id replen", cmd);
}

fn main() {
    // read command line
    let args: Vec<String> = env::args().collect();
    if args.len() < 5 {
        print_usage(&args[0]);
        return;
    }

    // read config file
    let f = args[2].parse::<u32>().unwrap();
    let id = args[3].parse::<u32>().unwrap();
    let replen = args[4].parse::<usize>().unwrap();
    let mut smr = rusty_bft::StateMachine::parse(&args[1], f, id);

    assert!(smr.is_replica(), "Invalid replica ID");

    println!(
        "Hello, world! I'm replica {}/{}: {:?}",
        smr.id,
        smr.n,
        smr.get_node(smr.id.try_into().unwrap())
    );

    smr.run_replica(&|_o: Vec<u8>| -> Vec<u8> {
        //println!("Replica {} has received operation {:?} to execute", id, _o);
        vec![0; replen]
    });
}
