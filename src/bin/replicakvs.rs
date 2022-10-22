use std::cell::RefCell;
use std::env;
use std::rc::Rc;

fn print_usage(cmd: &str) {
    eprintln!("Usage: {} config f id #crypto_threads #KVS_entries", cmd);
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
    let crypto_threads = args[4].parse::<usize>().unwrap();
    let kvs_entries = args[5].parse::<usize>().unwrap();
    let smr = rusty_bft::statemachine::Replica::new(&args[1], f, id, crypto_threads);

    println!(
        "Hello, world! I'm replica {}/{}: {:?}",
        smr.id,
        smr.n,
        smr.my_address()
    );

    let my_kvs = Rc::new(RefCell::new(rusty_bft::kvs::KVS::new(kvs_entries)));
    my_kvs.borrow_mut().populate();

    smr.run_replica(&|o: Vec<u8>| -> Vec<u8> {
        //println!("Replica {} has received operation {:?} to execute", id, o);
        let rep = my_kvs.borrow_mut().execute_operation(&o);
        //println!("Replica {} replies with {:?}", id, rep);
        rep
    });
}
