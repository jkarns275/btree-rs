use std;
use std::fs::File;
use std::io::{ Read, Write, Error, Seek, SeekFrom };
use std::collections::HashMap;
use raw_serde::*;
use btree::*;
use std::cmp::Ordering;
use std::io;
use priority_queue::PriorityQueue;

#[derive(RawSerialize, RawDeserialize, Copy, Clone, Debug)]
pub struct Node {
    pub parent: u64,
    pub loc: u64,
    pub len: u64,
    pub keys: [u64; NUM_KEYS],
    pub values: [u64; NUM_KEYS],
    pub children: [u64; NUM_CHILDREN],
    pub leaf: bool
}

impl Node {
    pub fn new() -> Node {
        Node {
            parent: NONE,
            len: 0,
            loc: 0,
            keys: [0; NUM_KEYS],
            values: [0; NUM_KEYS],
            children: [0; NUM_CHILDREN],
            leaf: true
        }
    }
}

struct Freq {
    pub freq: u64,
    pub loc: u64
}

impl Freq {
    pub fn new(loc: u64) -> Freq {
        Freq {
            freq: 1,
            loc: loc
        }
    }
}

impl Ord for Freq {
    fn cmp(&self, other: &Self) -> Ordering {
        other.freq.cmp(&self.freq)
    }
}

impl PartialOrd for Freq {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(other.freq.cmp(&self.freq))
    }
}

impl PartialEq for Freq {
    fn eq(&self, other: &Self) -> bool {
        self.loc == other.loc
    }
}

impl Eq for Freq {}


pub struct NodeCache {
    pub size: usize,
    freqs: PriorityQueue<Freq>,
    nodes: HashMap<u64, Node>
}

impl NodeCache {
    pub fn new(size: usize) -> Self {
        NodeCache {
            size,
            freqs: PriorityQueue::new(),
            nodes: HashMap::<u64, Node>::new()
        }
    }

    pub fn get<F: Read + Write + Seek>(&mut self, node_loc: u64, file: &mut F) -> Result<Node, io::Error> {
        if self.nodes.contains_key(&node_loc) {
            if self.freqs.update_key(Freq::new(node_loc), |x| x.freq += 1).is_err() {
                unreachable!();
            }
            Ok(self.nodes[&node_loc].clone())
        } else {
            let node;
            check!(NodeCache::read_node(node_loc, file), node);
            if self.nodes.len() < self.size {
                self.nodes.insert(node_loc, node);
                self.freqs.push(Freq::new(node_loc));
            } else {
                let lfu = self.freqs.poll().unwrap();
                self.nodes.remove(&lfu.loc);
                self.freqs.push(Freq::new(node_loc));
                self.nodes.insert(node_loc, node);
            }
            Ok(node)
        }
    }

    pub fn update(&mut self, node: &Node) {
        if self.nodes.contains_key(&node.loc) {
            *self.nodes.get_mut(&node.loc).unwrap() = node.clone();
        }
    }

    fn read_node<F: Read + Write + Seek>(pos: u64, file: &mut F) -> Result<Node, Error> {
        check!(file.seek(SeekFrom::Start(pos)));
        Node::raw_deserialize(file)
    }

}
