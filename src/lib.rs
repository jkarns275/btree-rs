extern crate raw_serde;

mod btree;
mod node;
mod priority_queue;
pub use btree::*;

#[test]
fn it_works() {
    let mut t = PBTree::<String, String>::new("heckaroo.dat").unwrap();
    use std::time::{ SystemTime };

    let x = 2048 * 16;

    let now = SystemTime::now();
    for i in 0..x {
        t.insert(&i.to_string(), &i.to_string()).unwrap();
    }
    match now.elapsed() {
        Ok(a) => println!("time to insert {} string string pairs: {:?}", x, a),
        Err(_) => panic!("Error measuring time.."),
    };

    let now = SystemTime::now();
    for i in 0..x {
        let x = t.search(&i.to_string()).unwrap().unwrap();
    }
    match now.elapsed() {
        Ok(a) => println!("time to search for {} string string pairs: {:?}", x, a),
        Err(_) => panic!("Error measuring time.."),
    };

}
