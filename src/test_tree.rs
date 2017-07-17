use std::fs::{ File, OpenOptions };
use std::io::{ Error, Seek, SeekFrom, Write };
use std::marker::PhantomData;
use raw_serde::*;
use file_buffer::*;
use std::fmt::Debug;
use node::{ Node, NodeCache };

pub const T: u64 = 16;
pub const T_USIZE: usize = T as usize;
pub const NUM_CHILDREN: usize = (T * 2) as usize;
pub const NUM_KEYS: usize = NUM_CHILDREN - 1;

pub const NONE: u64 = 0xFFFFFFFFFFFFFFFFu64;

pub struct TestTree<K, V> {
    pub treefile: File,
    pub keyfile: File,
    pub valfile: File,
    root_location: u64,
    pub root: Node,
    node_cache: NodeCache,
    phantom_k: PhantomData<K>,
    phantom_v: PhantomData<V>
}

impl<K, V> TestTree<K, V>
    where   K: RawSerialize + RawDeserialize + Eq + Ord + Debug,
            V: RawSerialize + RawDeserialize + Debug {

    pub fn new<S: Into<String>>(_path: S) -> Result<Self, Error> {
        let path = _path.into();

        let mut treefile;
        check!(OpenOptions::new().read(true).write(true).truncate(true).create(true).open(path.clone() + ".tree"), treefile);

        let keyfile;
        check!(OpenOptions::new().read(true).write(true).truncate(true).create(true).open(path.clone() + ".key"), keyfile);

        let valfile;
        check!(OpenOptions::new().read(true).write(true).truncate(true).create(true).open(path.clone() + ".val"), valfile);

        let mut root = Node::new();
        root.loc = 8;
        check!(treefile.seek(SeekFrom::Start(0)));
        // Location of root is written at the first 8 bytes of the treefile
        check!(8u64.raw_serialize(&mut treefile));
        // Write the first node, at the second 8 bytes of the treefile
        check!(root.raw_serialize(&mut treefile));

        Ok(TestTree {
            keyfile,
            valfile,
            treefile,
            root_location: 8,
            root,
            node_cache: NodeCache::new(128),
            phantom_k: PhantomData {},
            phantom_v: PhantomData {}
        })
    }

    pub fn set_cache_size(&mut self, size: usize) {
        self.node_cache.size = size;
    }

    pub fn open<S: Into<String>>(_path: S) -> Result<Self, Error> {
        let path = _path.into();

        let mut treefile;
        check!(OpenOptions::new().read(true).write(true).truncate(true).create(true).open(path.clone() + ".tree"), treefile);

        let keyfile;
        check!(OpenOptions::new().read(true).write(true).truncate(true).create(true).open(path.clone() + ".key"), keyfile);

        let valfile;
        check!(OpenOptions::new().read(true).write(true).truncate(true).create(true).open(path.clone() + ".val"), valfile);


        let root_location;
        check!(u64::raw_deserialize(&mut treefile), root_location);

        check!(treefile.seek(SeekFrom::Start(root_location)));
        let root;
        check!(Node::raw_deserialize(&mut treefile), root);

        Ok(TestTree {
            keyfile,
            valfile,
            treefile,
            root_location,
            node_cache: NodeCache::new(20),
            root,
            phantom_k: PhantomData {},
            phantom_v: PhantomData {}
        })
    }

    pub fn flush(&mut self) -> Result<(), Error> {
        check!(self.keyfile.flush());
        check!(self.valfile.flush());
        check!(self.treefile.flush());
        Ok(())
    }

    pub fn keys(&mut self) -> Result<Vec<K>, Error> {
        panic!("Do you really want to do that?")
    }

    fn split_child(&mut self, x: &mut Node, child: usize) -> Result<(), Error> {
        let mut y;
        check!(self.read_node(x.children[child]), y);

        y.parent = x.loc;

        let mut z = Node::new();
        z.leaf = y.leaf;
        z.len = T - 1;

        for j in 0 .. T_USIZE - 1 { z.keys[j] = y.keys[j + T_USIZE]; z.values[j] = y.values[j + T_USIZE]; }

        if !y.leaf { for j in 0..T_USIZE { z.children[j] = y.children[j + T_USIZE]; } }

        y.len = T - 1;

        for j in ((child + 1) as usize .. (x.len + 1) as usize).rev() { x.children[j + 1] = x.children[j]; }

        let z_loc;
        z.parent = x.loc;
        check!(self.write_node(&mut z), z_loc);
        x.children[child + 1] = z_loc;

        for j in (child as u64 .. x.len).rev() { x.keys[j as usize + 1] = x.keys[j as usize]; x.values[j as usize + 1] = x.values[j as usize]; }
        x.len += 1;
        x.keys[child] = y.keys[T_USIZE - 1];
        x.values[child] = y.values[T_USIZE - 1];

        check!(self.update_node(&x));
        check!(self.update_node(&y));

        Ok(())
    }

    pub fn insert(&mut self, k: &K, v: &V) -> Result<(), Error> {
        if self.root.len == NUM_KEYS as u64 {
            let mut s = Node::new();
            let s_loc;
            s.leaf = false;
            s.len = 0;
            s.children[0] = self.root_location;

            check!(self.write_node(&mut s), s_loc);
            self.root_location = s_loc;
            s.loc = s_loc;

            check!(self.treefile.seek(SeekFrom::Start(0)));
            check!(self.root_location.raw_serialize(&mut self.treefile));

            check!(self.split_child(&mut s, 0));
            check!(self.insert_nonfull(&mut s, k, v));

            self.root = s.clone();
            check!(self.update_node(&s));
        } else {
            let mut root = self.root.clone();
            check!(self.insert_nonfull(&mut root, k, v));
            self.root = root;
        }
        Ok(())
    }

    fn insert_nonfull(&mut self, x: &mut Node, k: &K, v: &V) -> Result<(), Error> {
        let mut i = x.len as i64;
        if x.leaf {
            if i > 0 {
                i -= 1;
                let mut k_i;
                check!(self.read_key(x.keys[i as usize]), k_i);
                while i >= 0 && *k < k_i {
                    x.keys[i as usize + 1] = x.keys[i as usize];
                    x.values[i as usize + 1] = x.values[i as usize];
                    i -= 1;
                    if i >= 0 { check!(self.read_key(x.keys[i as usize]), k_i); }
                }
                i += 1;
                let entry_loc;
                check!(self.write_entry(k, v), entry_loc);
                let (k_loc, v_loc) = entry_loc;
                x.keys[i as usize] = k_loc;
                x.values[i as usize] = v_loc;
                x.len += 1;
                check!(self.update_node(&*x));
                Ok(())
            } else {
                let entry_loc;
                check!(self.write_entry(k, v), entry_loc);
                let (k_loc, v_loc) = entry_loc;
                x.keys[0] = k_loc;
                x.values[0] = v_loc;
                x.len += 1;
                check!(self.update_node(&*x));
                Ok(())
            }
        } else {
            let mut k_i;
            i -= 1;
            check!(self.read_key(x.keys[i as usize]), k_i);
            while i >= 0 && *k < k_i {
                i -= 1;
                if i >= 0 { check!(self.read_key(x.keys[i as usize]), k_i); }
                else { break }
            }
            i += 1;
            let x_child_i;
            check!(self.node(x.children[i as usize]), x_child_i);
            if x_child_i.len == NUM_KEYS as u64 {
                check!(self.split_child(x, i as usize));
                let k_i;
                check!(self.read_key(x.keys[i as usize]), k_i);
                if k > &k_i { i += 1 }
            }
            let mut c_i;
            check!(self.node(x.children[i as usize]), c_i);
            self.insert_nonfull(&mut c_i, k, v)
        }
    }

    pub fn contains_key(&mut self, k: &K) -> Result<bool, Error> {
        let root = self.root_location;
        self.contains_key_rec(k, root)
    }

    fn contains_key_rec(&mut self, k: &K, pos: u64) -> Result<bool, Error> {
        let x;
        check!(self.node(pos), x);
        if x.len == 0 { return Ok(false) }

        let mut k_i: K;
        check!(self.read_key(x.keys[0]), k_i);

        let mut i = 0;
        while i < x.len && k > &k_i {
            i += 1;
            if i < x.len { check!(self.read_key(x.keys[i as usize]), k_i); }
        }

        if i < x.len && k == &k_i   { Ok(true) }
        else if x.leaf              { Ok(false) }
        else                        { self.contains_key_rec(k, x.children[i as usize]) }
    }

    pub fn search(&mut self, k: &K) -> Result<Option<V>, Error> {
        let r = self.root.clone();
        self.search_rec(r, k)
    }

    fn search_rec(&mut self, n: Node, k: &K) -> Result<Option<V>, Error> {
        if n.len == 0 { return Ok(None); }

        let mut k_i: K;
        check!(self.read_key(n.keys[0]), k_i);

        let mut i = 0;
        while i < n.len && k > &k_i {
            i += 1;
            if i < n.len {
                check!(self.read_key(n.keys[i as usize]), k_i);
            }
        }

        if i < n.len && *k == k_i {
            let ret;
            check!(self.read_value(n.values[i as usize]), ret);
            Ok(Some(ret))
        } else if n.leaf {
            Ok(None)
        } else {
            let next;
            check!(self.node(n.children[i as usize]), next);
            self.search_rec(next, k)
        }
    }

    #[inline(always)]
    fn write_entry(&mut self, k: &K, v: &V) -> Result<(u64, u64), Error> {
        let key_pos;
        let val_pos;
        check!(self.write_key(k), key_pos);
        check!(self.write_val(v), val_pos);

        Ok((key_pos, val_pos))

    }

    #[inline(always)]
    fn write_key(&mut self, k: &K) -> Result<u64, Error> {
        let pos;
        check!(self.keyfile.seek(SeekFrom::End(0)), pos);
        check!(k.raw_serialize(&mut self.keyfile));
        Ok(pos)
    }

    #[inline(always)]
    fn write_val(&mut self, v: &V) -> Result<u64, Error> {
        let pos;
        check!(self.valfile.seek(SeekFrom::End(0)), pos);
        check!(v.raw_serialize(&mut self.valfile));
        Ok(pos)
    }

    #[inline(always)]
    fn write_node(&mut self, node: &mut Node) -> Result<u64, Error> {
        let pos;
        check!(self.treefile.seek(SeekFrom::End(0)), pos);
        node.loc = pos;
        check!(node.raw_serialize(&mut self.treefile));
        Ok(pos)
    }

    #[inline(always)]
    fn update_node(&mut self, node: &Node) -> Result<(), Error> {
        check!(self.treefile.seek(SeekFrom::Start(node.loc)));
        check!(node.raw_serialize(&mut self.treefile));
        self.node_cache.update(node);
        Ok(())
    }

    #[inline(always)]
    fn node(&mut self, pos: u64) -> Result<Node, Error> {
        self.node_cache.get(pos, &mut self.treefile)
    }

    #[inline(always)]
    fn read_node(&mut self, pos: u64) -> Result<Node, Error> {
        check!(self.treefile.seek(SeekFrom::Start(pos)));
        Node::raw_deserialize(&mut self.treefile)
    }

    #[inline(always)]
    fn read_value(&mut self, pos: u64) -> Result<V, Error> {
        check!(self.valfile.seek(SeekFrom::Start(pos)));
        V::raw_deserialize(&mut self.valfile)
    }

    #[inline(always)]
    fn read_key(&mut self, pos: u64) -> Result<K, Error> {
        check!(self.keyfile.seek(SeekFrom::Start(pos)));
        K::raw_deserialize(&mut self.keyfile)
    }
}
