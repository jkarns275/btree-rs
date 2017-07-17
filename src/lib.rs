#![feature(rand)]
#![feature(box_syntax)]
extern crate raw_serde;
extern crate rand;

mod test_tree;
mod file_buffer;
mod btree;
mod node;
mod priority_queue;
pub use btree::*;
pub use test_tree::*;

#[test]
fn test_file_buffer_speed() {
    use std::time::{ SystemTime };
    use std::io::{ Error, Seek, SeekFrom, Write, Read, BufWriter};
    use std::collections::HashMap;
    #[allow(unused_imports)]
    use raw_serde::*;
    use std::cmp;
    use std::fs::{ File, OpenOptions };
    use file_buffer::*;

    let num_mb = 1024;

    let dat = vec![1u8; 1024];


    let mut t2 = OpenOptions::new().read(true).write(true).truncate(true).create(true).open("xasd.tree").unwrap();
    let mut file2 = BufWriter::new(t2);

    let now = SystemTime::now();

    for i in 0..num_mb * 1024 {
        file2.write(&dat[0..]).unwrap();
    }

    match now.elapsed() {
        Ok(a) => {
            let seconds = a.as_secs() as f64 + (a.subsec_nanos() as f64 / 1e9f64);
            println!("time to write {} megabytes with BufWriter: {:?}\n{} mb / s WRITE", num_mb, seconds, num_mb as f64 / seconds);
        },
        Err(_) => panic!("Error measuring time.."),
    };

    let mut t1 = OpenOptions::new().read(true).write(true).truncate(true).create(true).open("test.tree").unwrap();
    let mut file = BufFile::with_capacity(2, t1).unwrap();

    let now = SystemTime::now();

    for i in 0..num_mb * 1024 {
        file.write(&dat[0..]).unwrap();
    }

    match now.elapsed() {
        Ok(a) => {
            let seconds = a.as_secs() as f64 + (a.subsec_nanos() as f64 / 1e9f64);
            println!("time to write {} megabytes with BufFile: {:?}\n{} mb / s WRITE", num_mb, seconds, num_mb as f64 / seconds);
        },
        Err(_) => panic!("Error measuring time.."),
    };

}

#[test]
fn it_works() {
    return;
    let mut t = PBTree::<String, String>::new("heckaroo.dat").unwrap();
    use std::time::{ SystemTime };

    let x = 16*16*16*16;

    let now = SystemTime::now();
    for i in 0..x {
        let p = i.to_string();
        t.insert(&p, &p).unwrap();
    }
    match now.elapsed() {
        Ok(a) => println!("time to insert {} string string pairs: {:?}", x, a),
        Err(_) => panic!("Error measuring time.."),
    };

    let now = SystemTime::now();
    for i in 0..x {
        let x = t.search(&i.to_string()).unwrap().unwrap();
        //let y = test.search(&i.to_string()).unwrap().unwrap();
    }
    match now.elapsed() {
        Ok(a) => println!("time to search for {} string string pairs: {:?}", x, a),
        Err(_) => panic!("Error measuring time.."),
    };

    let mut t = TestTree::<String, String>::new("heckaroo.dat").unwrap();


    let now = SystemTime::now();
    for i in 0..x {
        let p = i.to_string();
        t.insert(&p, &p).unwrap();
    }
    match now.elapsed() {
        Ok(a) => println!("time to insert {} string string pairs: {:?}", x, a),
        Err(_) => panic!("Error measuring time.."),
    };

    let now = SystemTime::now();
    for i in 0..x {
        let x = t.search(&i.to_string()).unwrap().unwrap();
        //let y = test.search(&i.to_string()).unwrap().unwrap();
    }
    match now.elapsed() {
        Ok(a) => println!("time to search for {} string string pairs: {:?}", x, a),
        Err(_) => panic!("Error measuring time.."),
    };

}

#[test]
fn test_file_buffer() {
    use std::fs::{ File, OpenOptions };
    use std::io::{ Error, Seek, SeekFrom, Read };
    use std::marker::PhantomData;
    use raw_serde::*;
    use file_buffer::*;
    use node::{ Node, NodeCache };
    use std::time::{ SystemTime };
    use rand::Rng;

    let now = SystemTime::now();

    let mut test_file = OpenOptions::new().read(true).write(true).truncate(true).create(true).open("test_file_Asrfsddg").unwrap();
    let mut file = BufWriter::new(test_file);

    let mut rng = XorShiftRng::from_seed([0, 1, 377, 6712]);

    match now.elapsed() {
        Ok(a) => println!("time for test_file_buffer: {:?}", a),
        Err(_) => panic!("Error measuring time.."),
    };
}
