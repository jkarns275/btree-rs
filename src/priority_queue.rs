/// A generic PriorityQueue (e.g. a binary heap)
pub struct PriorityQueue<T> where T: Ord {
    /// A heap represented by a Vec.
    arr: Vec<T>,
}

/// Private methods
impl<T> PriorityQueue<T> where T: Ord {
    /// Calculates the index of the right child of ind. There is no gaurentee this is a valid index.
    #[inline(always)]
    fn right_child(ind: usize) -> usize { Self::left_child(ind) + 1 }

    /// Calculates the index of the left child of ind. There is no gaurentee this is a valid index.
    #[inline(always)]
    fn left_child(ind: usize)  -> usize { (ind << 1) + 1}

    /// Calculates the index of the parent of ind.
    #[inline(always)]
    fn parent(ind: usize)      -> usize { if ind == 0 { 0 } else { (ind - 1) >> 1 } }

    /// Fixes the heap after adding an element.
    /// Starts from the last index and swaps its way up until it is in a valid position.
    fn adjust_after_push(&mut self) {
        let mut index = self.arr.len() - 1;
        while index != 0 {
            let parent = Self::parent(index);
            if self.arr[index] < self.arr[parent] {
                self.arr.swap(index, parent);
                index = parent;
            } else {
                break;
            }
        }
    }

    /// Fixes a node that is potentially in the wrong spot.
    fn adjust_after_decrease(&mut self, mut index: usize) {
        loop {
            let left = Self::left_child(index);
            let right = Self::right_child(index);
            if left >= self.arr.len() { break; }
            let least = {
                if right >= self.arr.len() || self.arr[left] < self.arr[right] { left }
                else { right }
            };
            if self.arr[index] > self.arr[least] {
                self.arr.swap(index, least);
                index = least;
                continue;
            }
            break;
        }
    }

    /// Fixes the node in position 0 after removing the first element and replacing it with the last (in the poll method)
    fn adjust_after_poll(&mut self) {
        self.adjust_after_decrease(0);
    }

    fn search(&self, item: &T) -> Option<usize> {
        //self.in_search(item, 0)
        let mut ind = 0;
        loop {
            if self.arr[ind] == *item {
                return Some(ind)
            } else if self.arr[ind] > *item {
                return None
            } else {
                ind += 1;
            }
        }
     }
}

/// Public methods for PriorityQueue.
#[allow(dead_code)]
impl<T> PriorityQueue<T> where T: Ord {
    /// Creates a new empty priority queue
    pub fn new() -> Self {
        PriorityQueue {
            arr: vec![]
        }
    }

    // Applies a function to a key and moves it to the proper position in the heap.
    pub fn update_key<F>(&mut self, key: T, apply: F) -> Result<(), ()>
        where F: Fn(&mut T) -> () {
        let ind = self.search(&key);
        match ind {
            Some(ind) => {
                apply(&mut self.arr[ind]);
                self.adjust_after_decrease(ind);
                Ok(())
            },
            None => Err(())
        }
    }

    /// Adds an element to the heap and ensures it is still a heap; if not it makes it so.
    pub fn push(&mut self, item: T) {
        self.arr.push(item);
        self.adjust_after_push();
    }

    /// Pushes every element in items onto the queue, ordering them accordingly.
    pub fn append<I>(&mut self, items: I) where I: Iterator<Item=T> {
        for item in items {
            self.push(item);
        }
    }

    /// Checks if the queue contains the element
    pub fn contains(&self, item: &T) -> bool {
        match self.search(item) {
            Some(_) => true,
            None => false
        }
    }

    /// Removes an element to the heap and ensures it is still a heap; if not it makes it so.
    pub fn poll(&mut self) -> Option<T> {
        if self.arr.len() == 0 {
            None
        } else if self.arr.len() == 1 {
            self.arr.pop()
        } else {
            let ind = self.arr.len() - 1;
            self.arr.swap(0, ind);
            let result = self.arr.pop();
            self.adjust_after_poll();
            result
        }
    }

    /// Returns true of the queue is empty, otherwise false.
    pub fn empty(&self) -> bool {
        self.arr.len() == 0
    }
}
