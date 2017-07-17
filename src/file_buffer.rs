use std::cmp::{ Ord, Ordering };
use std::fs::File;
use std::io::{ Error, Seek, SeekFrom, Write, Read };
use std::collections::HashMap;
#[allow(unused_imports)]
use raw_serde::*;
use std::cmp;

/// Slab size MUST be a power of 2!
const SLAB_SIZE: usize = 1024*1024; // 1 Megabyte
/// Used to turn a file index into an array index (since SLAB_SIZE is a power of two,
/// subtracting one from it will yield all ones, and anding it with a number will
/// yield only the lowest n bits, where SLAB_SIZE = 2^n
const SLAB_MASK: u64 = SLAB_SIZE as u64 - 1;

const DEFAULT_NUM_SLABS: usize = 16;

/// A struct representing a section of a file
pub struct Slab {
    /// The data
    pub dat: Vec<u8>,
    /// First byte in the file that is contained in this slab
    start: u64,
    /// Number of times this slab has been accessed.
    uses: u64
}

impl Slab {
    /// Creates a new slab, drawing it's data from the given file at the given location
    /// Location should be at the beginning of a slab (e.g. a muitiple of SLAB_SIZE)
    pub fn new(loc: u64, file: &mut File) -> Result<Slab, Error> {
        check!(file.seek(SeekFrom::Start(loc)));
        let mut dat = vec![0u8; SLAB_SIZE];
        check!(file.read(&mut dat[0..]));
        Ok(Slab {
            dat: dat,
            start: loc,
            uses: 0
        })
    }

    /// Write the slab to disk
    pub fn write(&self, file: &mut File) -> Result<(), Error> {
        check!(file.seek(SeekFrom::Start(self.start)));
        check!(file.write_all(&self.dat[0..]));
        Ok(())
    }
}

pub struct BufFile {
    /// The maximum number of slabs this BufFile can have
    slabs: usize,
    /// Used to quickly map a file index to an array index (to index self.dat)
    map: HashMap<u64, usize>,
    /// Contains the actual slabs
    pub dat: Vec<Slab>,
    /// The file to be written to and read from
    file: File,
    /// Represents the current location of the cursor.
    /// This does not reflect the actual location of the cursor in the file.
    pub cursor: u64,
    /// The file index that is the end of the file.
    pub end: u64
}

impl BufFile {
    /// Creates a new BufFile.
    pub fn new(mut file: File) -> Result<BufFile, Error> {
        Self::with_capacity(DEFAULT_NUM_SLABS, file)
    }

    /// Creates a new BufFile with the specified number of slabs.
    pub fn with_capacity(slabs: usize, mut file: File) -> Result<BufFile, Error> {
        // Find the end of the file, in case the file isnt empty.
        let end;
        check!(file.seek(SeekFrom::End(0)), end);

        // Move the cursor back to the start of the file.
        check!(file.seek(SeekFrom::Start(0)));
        Ok(BufFile {
            slabs: slabs,  // Maximum of 32 slabs
            dat: vec![],
            map: HashMap::new(),
            file,
            cursor: 0,  // Since the cursor is at the start of the file
            end
        })
    }

    /// Finds the slab that contains file index loc, if it doesn't exist None
    /// is returned. If it does exist, Some(index) is returned, where index
    /// is an index into self.dat.
    fn find_slab(&self, loc: u64) -> Option<usize> {
        let start = (loc | SLAB_MASK) ^ SLAB_MASK;
        if self.map.contains_key(&start) {
            let x = self.map[&start].clone();
            Some(x)
        } else {
            None
        }
    }

    /// Adds a slab to the BufFile, if it isn't already present. It will write
    /// the least frequently used slab to disk and load the new one into self.dat,
    /// then return Ok(index), index being an index for self.dat.
    fn add_slab(&mut self, loc: u64) -> Result<usize, Error> {
        let start = (loc | SLAB_MASK) ^ SLAB_MASK;
        if self.map.contains_key(&start) {
            return Ok(self.map[&start].clone());
        }
        // Add up to 2048 bytes if the file is not long enough for this incoming location
        let len = self.end as usize;
        // The end if the file is not as long as it needs to be, write some dummy data (0's) to extend it
        // This behavior will allow some strange behavior through, but it shouldnt't really be harmful
        if len < start as usize + SLAB_SIZE && len < loc as usize {
            let i = vec![0; SLAB_SIZE];
            let dif = len & SLAB_MASK as usize;
            check!(self.file.write_all(&i[0..SLAB_SIZE - dif]));
            self.end = loc + 1;
        }
        // If we're not at the maximum number of slabs, make a new one,
        // and add it to dat and to the map
        if self.dat.len() < self.slabs {
            let ind = self.dat.len();
            match Slab::new(start, &mut self.file) {
                Ok(x) => {
                    self.map.insert(start, self.dat.len());
                    self.dat.push(x);
                    Ok(ind)
                },
                Err(e) => Err(e)
            }
        }

        // We are at the maximum number of slabs - one of them must be removed
        else {
            // Find the minimum - we have to go through all of them, there isn't
            // a simple solution to avoid this that can easily be implemented.
            // (maybe fibonacci heap?)
            let mut min = 0;
            for i in 0..self.slabs {
                if self.dat[min].uses == 1 {
                    min = i;
                    // The minimum number of reads is 1, so if we encounter 1 just break.
                    break;
                }
                if self.dat[min].uses > self.dat[i].uses {
                    min = i;
                }
            }
            // Make a new slab, write the old one to disk, replace old slab
            match Slab::new(start, &mut self.file) {
                Ok(x) => {
                    // Write the old slab to disk
                    check!(self.dat[min].write(&mut self.file));

                    // Move the cursor back to where it was
                    self.file.seek(SeekFrom::Start(self.cursor));

                    // Remove the old slab from the map
                    self.map.remove(&self.dat[min].start);

                    // Add the new one
                    self.map.insert(start, min);

                    // Assign the new value
                    self.dat[min] = x;
                    Ok(min)
                },
                Err(x) => Err(x)
            }
        }
    }
}

impl Read for BufFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        // If the place the cursor will be after the read is in the same slab as it will be during the beginning,
        // and the length of the buffer is less than SLAB_SIZE
        if buf.len() <= SLAB_SIZE
            && (((buf.len() as u64 + self.cursor - 1) | SLAB_MASK) ^ SLAB_MASK == (self.cursor | SLAB_MASK) ^ SLAB_MASK)
            {
            // The index in self.dat (which slab to use)
            let index;
            let cursor = self.cursor;

            // If we dont find it, add a new one!
            match self.find_slab(cursor) {
                Some(x) => index = x,
                None => match self.add_slab(cursor) {
                    Ok(x) => index = x,
                    Err(e) => return Err(e)
                }
            };

            // We're using this slab, so increment its use count
            self.dat[index].uses += 1;
            {
                // Since we're indexing, only use the lower bits n as index.
                let masked = (self.cursor & SLAB_MASK) as usize;
                let mut slice = &mut self.dat[index].dat[masked as usize .. masked as usize + buf.len()];
                buf.clone_from_slice(slice);
            }

            // Move the cursor
            self.cursor += buf.len() as u64;

            // Return the number of bytes read
            Ok(buf.len())
        }
        // the data is contained in more than one slab, and may be larger than SLAB_SIZE bytes
        else {
            // How many times does SLAB_SIZE go into slabs? Thats the lower limit on how many slabs we have to read from
            let mut slabs = buf.len() / SLAB_SIZE;
            // If there is a remainder
            if buf.len() as u64 & SLAB_MASK > 0 { slabs += 1; }
            // There is less than SLAB_SIZE bytes to read, but it is spread out over 2 slabs
            if slabs == 1 { slabs = 2; }

            let mut bytes_read = 0;
            // For each slab we have to go through
            for _ in 0..slabs {
                // How many bytes to we have to read this iteration? Either the rest of the data or the rest of a slab
                let mut to_read = cmp::min(SLAB_SIZE - (self.cursor & SLAB_MASK) as usize, buf.len() - bytes_read);
                // if cursor is a multiple of SLAB_SIZE then cursor & slab_mask will be 0

                // Which slab to read from
                let index;
                // Combat the borrow-checker
                let cursor = self.cursor;
                // If our slab is already present, cool, if not, add it
                match self.find_slab(cursor) {
                    Some(x) => index = x,
                    None =>
                        match self.add_slab(cursor) {
                            Ok(x) => index = x,
                            Err(e) => return Err(e)
                        }
                };
                // We're using the slab so increment the use count
                self.dat[index].uses += 1;
                {
                    let masked = (self.cursor & SLAB_MASK) as usize;
                    let mut slice = &mut self.dat[index].dat[masked as usize .. masked as usize + to_read];
                    let mut target = &mut buf[bytes_read .. bytes_read + to_read];
                    target.clone_from_slice(slice);
                }
                self.cursor += to_read as u64;
                bytes_read += to_read;
            }

            Ok(buf.len())
        }
    }
}

impl Write for BufFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        // If the place the cursor will be after the write is in the same slab as it will be during the beginning,
        // and the length of the buffer is less than SLAB_SIZE
        if buf.len() <= SLAB_SIZE
        && (((buf.len() as u64 + self.cursor - 1) | SLAB_MASK) ^ SLAB_MASK == (self.cursor | SLAB_MASK) ^ SLAB_MASK)
        {
            // The index in self.dat (which slab to use)
            let index;
            let cursor = self.cursor;

            // If we dont find it, add a new one!
            match self.find_slab(cursor) {
                Some(x) => index = x,
                None => match self.add_slab(cursor) {
                    Ok(x) => index = x,
                    Err(e) => return Err(e)
                }
            };

            // We're using this slab, so increment its use count
            self.dat[index].uses += 1;
            {
                // Since we're indexing, only use the lower bits n as index.
                let masked = (self.cursor & SLAB_MASK) as usize;
                let mut slice = &mut self.dat[index].dat[masked as usize .. masked as usize + buf.len()];
                slice.clone_from_slice(buf);
            }

            // Move the cursor
            self.cursor += buf.len() as u64;

            // Return the number of bytes written
            Ok(buf.len())
        }
        // the data is contained in more than one slab, and may be larger than SLAB_SIZE bytes
        else {
            // How many times does SLAB_SIZE go into slabs? Thats the lower limit on how many slabs we have to read from
            let mut slabs = buf.len() / SLAB_SIZE;
            // If there is a remainder
            if buf.len() as u64 & SLAB_MASK > 0 { slabs += 1; }
            // There is less than SLAB_SIZE bytes to read, but it is spread out over 2 slabs
            if slabs == 1 { slabs = 2; }

            let mut bytes_written = 0;
            // For each slab we have to go through
            for _ in 0..slabs {
                // How many bytes to we have to read this iteration? Either the rest of the data or the rest of a slab
                let mut to_write = cmp::min(SLAB_SIZE - (self.cursor & SLAB_MASK) as usize, buf.len() - bytes_written);
                // if cursor is a multiple of SLAB_SIZE then cursor & slab_mask will be 0

                // Which slab to read from
                let index;
                // Combat the borrow-checker
                let cursor = self.cursor;
                // If our slab is already present, cool, if not, add it
                match self.find_slab(cursor) {
                    Some(x) => index = x,
                    None =>
                        match self.add_slab(cursor) {
                            Ok(x) => index = x,
                            Err(e) => return Err(e)
                        }
                };
                // We're using the slab so increment the use count
                self.dat[index].uses += 1;
                {
                    let masked = (self.cursor & SLAB_MASK) as usize;
                    let mut slice = &mut self.dat[index].dat[masked as usize .. masked as usize + to_write];
                    let mut target = &buf[bytes_written .. bytes_written + to_write];
                    slice.clone_from_slice(target);
                }
                self.cursor += to_write as u64;
                bytes_written += to_write;
            }

            Ok(buf.len())
        }
    }

    fn flush(&mut self) -> Result<(), Error> {
        for slab in self.dat.iter() {
            check!(slab.write(&mut self.file))
        }
        Ok(())
    }
}

impl Seek for BufFile {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, Error> {
        match pos {
            SeekFrom::Start(x) => {
                if self.find_slab(x).is_none() {
                    let cursor = self.cursor;
                    match self.add_slab(cursor) {
                        Ok(_) => {},
                        Err(e) => return Err(e)
                    }
                }
                self.cursor = x;
                Ok(self.cursor)
            },
            SeekFrom::End(x) => {
                self.cursor =
                    if x < 0 { self.end - (-x) as u64 }     // This would be an error if buffers / files
                    else { self.end - x as u64 };           // weren't automatically extended beyond
                                                            // the end.
                let cursor = self.cursor;
                if self.find_slab(cursor).is_none() {
                    match self.add_slab(cursor) {
                        Ok(_) => {},
                        Err(e) => return Err(e)
                    }
                }

                Ok(cursor)
            },
            SeekFrom::Current(x) => {
                let cur = self.cursor;

                let cursor =
                    if x < 0 { cur - (-x) as u64 }
                    else { cur - x as u64 };
                self.cursor = cursor;

                if self.find_slab(cursor).is_none() {
                    match self.add_slab(cursor) {
                        Ok(_) => {},
                        Err(e) => return Err(e)
                    }
                }

                Ok(self.cursor)
            }
        }
    }
}

impl Drop for BufFile {
     fn drop(&mut self) {
         self.flush();
     }
}
