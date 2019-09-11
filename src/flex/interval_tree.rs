//! # Interval tree
//!
//! This mod contains interval tree implemented as a red-black tree.
//!
//! It features:
//!
//! * fast query about overlapping intervals
//! * interval split and merge

#![allow(unused)]

use std::cmp::Ordering::*;
use slab::Slab;
use std::mem;
use std::ptr;

/// This trait defines a merge operator, which is used to merge a pair
/// of values of the same type into a single value.
pub trait Merge {
    /// lhs * self
    fn merge_left(&mut self, lhs: Self);

    /// self * rhs
    fn merge_right(&mut self, rhs: Self);
}

/// This trait defines a split operator, which is used to split a
/// value into a pair of values of the same type.
pub trait Split<Pos>: Sized {
    fn split(self, pos: Pos) -> (Self, Self);
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum Color {
    Red,
    Black,
}
use Color::*;

pub type Index = usize;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum FindResult {
    ///     ====
    /// --      --
    Miss,

    /// Equal
    Equal(Index),

    ///   ===
    /// ------
    Outside(Index),

    /// ====
    ///  --
    Inside(Index),

    ///  ====
    /// --
    Left(Index),

    /// ====
    ///   ---
    Right(Index),
}

pub enum FindKey {
    Right(Index),
    Left(Index),
    This(Index),
}

#[derive(Clone)]
pub struct IntervalTree<K, V>
where
    K: Eq + Ord + Copy,
    V: Clone + Merge,
{
    root: Option<Index>,
    tree: Slab<Node<K, V>>,
}

/// A TreeNode represents an interval [a, b).
#[derive(Clone)]
struct Node<K, V>
where
    K: Eq + Ord + Copy,
    V: Clone + Merge,
{
    color: Color,
    a: K, // key
    b: K,
    m: K,
    value: V,
    l: Option<Index>, // left
    r: Option<Index>, // right
    p: Option<Index>, // parent
}

impl<K,V> IntervalTree<K,V>
where
    K: Eq + Ord + Copy,
    V: Clone + Merge,
{
    pub fn new() -> IntervalTree<K,V> {
        Self::with_capacity(0)
    }

    pub fn with_capacity(cap: usize) -> IntervalTree<K,V> {
        IntervalTree {
            tree: Slab::with_capacity(cap),
            root: None,
        }
    }

    /// Insert a new interval; if there's an overlapping interval,
    /// merge with it.
    pub fn insert(&mut self, a: K, b: K, value: V) {
        match self.find(a, b) {
            FindResult::Miss => {
                self.insert_nonoverlapping(a, b, value)
            }
            FindResult::Left(n) => {
                unimplemented!()
            }
            FindResult::Right(n) => {
                unimplemented!()
            }
            FindResult::Outside(n) => {
                unimplemented!()
            }
            FindResult::Inside(n) => {
                unimplemented!()
            }
            FindResult::Equal(n) => {
                self.node_mut(n).value = value;
            }
        }
    }

    /// Insert a new interval into this tree.
    ///
    /// This interval [a,b) should not overlap with any intervals.
    pub fn insert_nonoverlapping(&mut self, a: K, b: K, value: V) {
        assert!(b > a);

        let mut node = Node::new(a, b, value);
        let mut id;

        match self.root_mut() {
            None => {
                id = self.tree.insert(node) as Index;
                self.root = Some(id);
            }
            Some((root_id, root)) => {
                let find = self.find_key_update(root_id, a, b);
                id = self.tree.insert(node) as Index;
                match find {
                    FindKey::Left(parent_id) => {
                        self.node_mut(parent_id).l = Some(id);
                        self.node_mut(id).p = Some(parent_id);
                    }
                    FindKey::Right(parent_id) => {
                        self.node_mut(parent_id).r = Some(id);
                        self.node_mut(id).p = Some(parent_id);
                    }
                    FindKey::This(_) => {
                        // It's assumed no intervals overlap with each
                        // other.
                        unreachable!()
                    }
                }
            }
        }

        self.repair_after_insert(id);
    }

    /// Remove node del from the tree.
    pub fn remove(&mut self, del: Index) {
        let repl = self.smallest_right(del);

        if repl.is_none() && self.node(del).l.is_none() {
            self.free(del);
            self.root = None;
            return
        } else if repl.is_none() && self.node(del).l.is_some() {
            self.free(del);
            self.root = self.node(del).l;
            self.root_mut().unwrap().1.p = None;
            return
        }

        // Swap del and r, moving down the del to a better place.
        let repl = repl.unwrap();
        self.swap(del, repl);

        // Then delete repl (was del).
        self.unlink_one(repl);
        self.free(repl);

        // Maintain
        
    }

    /// Swaps the contents of node p and q, keeping the original index
    /// and pointer.
    fn swap(&mut self, p: Index, q: Index) {
        unsafe {
            ptr::swap(&mut self.tree[p], &mut self.tree[q]);
            ptr::swap(&mut self.tree[p].p, &mut self.tree[q].p);
        }
    }

    /// Unlink a node with only right child or no children.
    ///
    /// `left` indicates whether n is the left child (-1), or right child (1).
    fn unlink_one(&mut self, n: Index) {
        let r = self.node(n).r; // right child of n, taking place of n

        // n is Red, so no children. n is safely deleted.
        if self.node(n).color == Red {
            return
        }

        // r takes the place of n.
        if let Some(r) = r {
            self.replace(n, r);
        }

        // n is Black
        self.bubble_black(r.unwrap());
    }

    /// Node n is colored double black. Bubble the extraneous black up
    /// until it's consumed.
    fn bubble_black(&mut self, mut n: Index) {
        loop {
            if self.node(n).p.is_none() {
                break
            } else if self.node(n).color == Red {
                self.node_mut(n).color = Black;
                break
            }

            // n is Black and is not root
            let (s, p) = self.sibling_and_parent(n).unwrap();
            let p_color = self.node(p).color;
            let sl = self.node(s).l;
            let sr = self.node(s).r;

            if self.node(s).color == Red {
                self.rotate_up(s);
                self.node_mut(s).color = Black;
                self.node_mut(p).color = Red;
                continue
            } else if sl.is_some() && self.node(sl.unwrap()).color == Red {
                self.rotate_up(sl.unwrap());
                self.rotate_up(sl.unwrap());
                self.node_mut(sl.unwrap()).color = p_color;
                break
            } else if sr.is_some() && self.node(sr.unwrap()).color == Red {
                self.rotate_up(s);
                self.node_mut(s).color = p_color;
                self.node_mut(sr.unwrap()).color = Black;
                break
            } else {
                self.node_mut(s).color = Red;
                self.node_mut(p).color = Black;
                if self.node(p).color == Black {
                    n = p;
                    continue;
                } else {
                    break;
                }
            }
        }
    }

    /// Replace node u with v.
    ///
    /// Panics if u is root.
    fn replace(&mut self, u: Index, v: Index) {
        let up = self.node(u).p.unwrap();
        self.node_mut(v).p = Some(up);
        if self.node(up).l == Some(u) {
            self.node_mut(up).l = Some(v);
        } else {
            self.node_mut(up).r = Some(v);
        }
        self.update(up);
    }

    /// Take node out of slab. This should be the last step of the
    /// removal of a node. This does not handle any metadata.
    fn free(&mut self, id: Index) -> Node<K,V> {
        self.tree.remove(id)
    }

    fn smallest_right(&self, mut cur: Index) -> Option<Index> {
        if let Some(r) = self.node(cur).r {
            cur = r;
        } else {
            return None;
        }
        while let Some(l) = self.node(cur).l {
            cur = l;
        }
        Some(cur)
    }

    /// Find a node by key, and update the info along the path.  It
    /// either finds a node with the specified key, or, if such node
    /// doesn't exist, finds a node which is to be the parent of such
    /// node.
    fn find_key_update(&mut self, mut cur: Index, a: K, b: K) -> FindKey {
        loop {
            let node = self.node_mut(cur);
            node.m = node.m.max(b);
            match a.cmp(&node.a) {
                Equal => {
                    break FindKey::This(cur)
                }
                Greater if node.r.is_some() => {
                    cur = node.r.unwrap();
                }
                Greater => {
                    break FindKey::Right(cur)
                }
                Less if node.l.is_some() => {
                    cur = node.l.unwrap();
                }
                Less => {
                    break FindKey::Left(cur)
                }
            }
        }
    }

    /// Find an interval overlapping with [a, b) quickly.
    pub fn find(&self, a: K, b: K) -> FindResult {
        let dest = self.find_overlapping(a, b);
        match dest {
            None => FindResult::Miss,
            Some(dest) => {
                let node = self.node(dest);
                debug_assert!(b > node.a && a < node.b);
                match (a.cmp(&node.a), b.cmp(&node.b)) {
                    (Equal, Equal) => {
                        return FindResult::Equal(dest)
                    }
                    (Less, Greater) | (Less, Equal) | (Equal, Greater) => {
                        return FindResult::Outside(dest)
                    }
                    (Greater, Less) | (Greater, Equal) | (Equal, Less) => {
                        return FindResult::Inside(dest)
                    }
                    (Less, Less) => {
                        return FindResult::Left(dest)
                    }
                    (Greater, Greater) => {
                        return FindResult::Right(dest)
                    }
                }
            }
        }
    }

    fn find_overlapping(&self, a: K, b: K) -> Option<Index> {
        let mut cur = self.root;
        while let Some(node) = cur {
            let node = self.node(node);
            if b > node.a && a < node.b {
                return cur
            }
            if node.l.is_some() && a <= self.node(node.l.unwrap()).m {
                cur = node.l;
            } else {
                cur = node.r;
            }
        }
        None
    }

    fn root(&self) -> Option<(Index, &Node<K,V>)> {
        self.root.map(|x| (x, unsafe { self.node(x) }))
    }

    fn root_mut(&mut self) -> Option<(Index, &mut Node<K,V>)> {
        self.root.map(move |x| (x, unsafe { self.node_mut(x) }))
    }

    fn repair_after_insert(&mut self, mut n: Index) {
        if let Some(mut p) = self.node(n).p {
            // In this case, nothing has to be repaired.
            if self.node(p).color == Black {
                return
            }
            // Both n and p are red. In this case, the grandparent
            // exists.
            match self.uncle_and_grandparent(n) {
                Some((u, g)) if self.node(u).color == Red => {
                    // Uncle is red.
                    self.node_mut(u).color = Black;
                    self.node_mut(p).color = Black;
                    self.node_mut(g).color = Red;
                    return self.repair_after_insert(g);
                }
                _ => {
                    // Move the new node outside.
                    let g = self.node(p).p.unwrap();
                    if self.node(p).r == Some(n) && self.node(g).l == Some(p) {
                        self.rotate_left(p);
                        p = n;
                        n = self.node(n).l.unwrap();
                    } else if self.node(p).l == Some(n) && self.node(g).r == Some(p) {
                        self.rotate_right(p);
                        p = n;
                        n = self.node(n).r.unwrap();
                    }
                    // Move the grandparent down, the parent up
                    if Some(n) == self.node(p).l {
                        self.rotate_right(g);
                    } else {
                        self.rotate_left(g);
                    }
                    self.node_mut(p).color = Black;
                    self.node_mut(g).color = Red;
                }
            }
        }

        self.root_mut().unwrap().1.color = Black;
    }

    fn node(&self, id: Index) -> &Node<K,V> {
        &self.tree[id]
    }

    fn node_mut(&mut self, id: Index) -> &mut Node<K,V> {
        &mut self.tree[id]
    }

    /// Move the node up by rotating. Panics if is root.
    fn rotate_up(&mut self, o: Index) {
        let p = self.node(o).p.unwrap();
        if Some(o) == self.node(p).l {
            self.rotate_right(p);
        } else {
            self.rotate_left(p);
        }
    }

    /// Panics if o.r is None.
    fn rotate_left(&mut self, o: Index) {
        let s = self.node(o).r.unwrap();
        let t = self.node(s).l;

        match self.node(o).p {
            None => self.root = Some(s),
            Some(p) => {
                if Some(o) == self.node(p).l {
                    self.node_mut(p).l = Some(s)
                } else {
                    self.node_mut(p).r = Some(s)
                }
            }
        }
        self.node_mut(s).p = self.node(o).p;
        self.node_mut(o).p = Some(s);
        if let Some(t) = t {
            self.node_mut(t).p = Some(o);
        }

        self.node_mut(o).r = t;
        self.node_mut(s).l = Some(o);

        self.update(o);
        self.update(s);
    }

    /// Panics if o.l is None.
    fn rotate_right(&mut self, o: Index) {
        let s = self.node(o).l.unwrap();
        let t = self.node(s).r;

        match self.node(o).p {
            None => self.root = Some(s),
            Some(p) => {
                if Some(o) == self.node(p).l {
                    self.node_mut(p).l = Some(s);
                } else {
                    self.node_mut(p).r = Some(s);
                }
            }
        }
        self.node_mut(s).p = self.node(o).p;
        self.node_mut(o).p = Some(s);
        if let Some(t) = t {
            self.node_mut(t).p = Some(o);
        }

        self.node_mut(o).l = t;
        self.node_mut(s).r = Some(o);

        self.update(o);
        self.update(s);
    }

    fn sibling_and_parent(&self, n: Index) -> Option<(Index, Index)> {
        self.node(n).p
            .and_then(|p| {
                if Some(n) == self.node(p).l {
                    self.node(p).r.map(|r| (r, p))
                } else {
                    self.node(p).l.map(|l| (l, p))
                }
            })
    }

    fn uncle_and_grandparent(&self, n: Index) -> Option<(Index, Index)> {
        self.node(n).p
            .and_then(|p| self.sibling_and_parent(p))
    }

    /// Returns true if the value is updated.
    fn update(&mut self, n: Index) -> bool {
        let node = self.node(n);
        let mut m = node.b;
        if let Some(l) = node.l {
            m = m.max(self.node(l).m);
        }
        if let Some(r) = node.r {
            m = m.max(self.node(r).m);
        }
        if self.node_mut(n).m != m {
            self.node_mut(n).m = m;
            true
        } else {
            false
        }
    }
}

impl<K,V> Node<K,V>
where
    K: Eq + Ord + Copy,
    V: Clone + Merge,
{
    fn new(a: K, b: K, value: V) -> Node<K,V> {
        Node {
            color: Red,
            a, b, value,
            m: b,
            l: None, r: None, p: None
        }
    }

    /// Look for an overlapping interval [a,b) in this node and its
    /// children.  If there is more than one interval overlapping
    /// [a,b), only the first one is returned.  `index` is the index
    /// of self in `tree`.
    fn find(&self, index: Index, tree: &Slab<Node<K,V>>, a: K, b: K) -> FindResult {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    extern crate test;
    use test::Bencher;
    use super::*;

    impl Merge for i32 {
        fn merge_left(&mut self, lhs: i32) {
            *self += lhs
        }

        fn merge_right(&mut self, rhs: i32) {
            *self += rhs
        }
    }

    fn print_tree(t: &IntervalTree<i32, i32>, cur: Option<Index>, level: u32) {
        if let Some(cur) = cur {
            let cur = cur as usize;
            print_tree(t, t.tree[cur].l, level + 1);

            (0..level).for_each(|_| print!("\t\t"));
            println!("{:?},{},{}", t.tree[cur].color, t.tree[cur].a, t.tree[cur].m-1);

            print_tree(t, t.tree[cur].r, level + 1);
        }
    }

    fn sanity_check(t: &IntervalTree<i32, i32>) {
        if t.root.is_none() {
            return
        }
        count_black(t, t.root);
        let mut found_root = false;
        for (i, node) in t.tree.iter() {
            // Root check
            if node.p.is_none() {
                assert_eq!(found_root, false);
                assert_eq!(t.root, Some(i as Index));
                assert_eq!(node.color, Black);
                found_root = true;
            }
            // BST check
            if let Some(l) = node.l {
                assert!(t.node(l).a <= node.a);
                assert_eq!(t.node(l).p, Some(i as Index));
                assert_eq!(t.node(t.node(l).p.unwrap()).l, Some(l));
            }
            if let Some(r) = node.r {
                assert!(t.node(r).a >= node.a);
                assert_eq!(t.node(r).p, Some(i as Index));
                assert_eq!(t.node(t.node(r).p.unwrap()).r, Some(r));
            }
            // Color check
            if node.color == Red {
                let l = node.l.map(|l| t.node(l).color == Black).unwrap_or(true);
                let r = node.r.map(|r| t.node(r).color == Black).unwrap_or(true);
                if !l || !r {
                    println!("===================");
                    println!("Sanity check failed");
                    println!("===================");
                    print_tree(&t, t.root, 0);
                    panic!("A red node has a red child");
                }
            }
            // Info check
            let mut m = node.b;
            if let Some(l) = node.l {
                m = m.max(t.node(l).m);
            }
            if let Some(r) = node.r {
                m = m.max(t.node(r).m);
            }
            assert_eq!(node.m, m);
        }
        assert_eq!(found_root, true);
    }

    fn count_black<K,V>(t: &IntervalTree<K,V>, cur: Option<Index>) -> u32
    where K: Ord + Copy + Eq, V: Clone + Merge,
    {
        match cur {
            None => 1,
            Some(n) => {
                let n = unsafe { t.node(n) };
                assert_eq!(count_black(t, n.l), count_black(t, n.r));
                if n.color == Red {
                    count_black(t, n.l)
                } else {
                    1 + count_black(t, n.l)
                }
            }
        }
    }

    #[test]
    fn rb_insert0() {
        let mut t = IntervalTree::new();
        sanity_check(&t);

        t.insert(10, 11, 0); // 0
        sanity_check(&t);

        t.insert(11, 12, 0); // 1
        sanity_check(&t);

        t.insert(6, 7, 0); // 2
        sanity_check(&t);

        t.insert(4, 5, 0); // 3
        sanity_check(&t);

        t.insert(2, 3, 0); // 4
        sanity_check(&t);
    }

    #[test]
    fn rb_insert1() {
        let mut t = IntervalTree::new();
        assert_eq!(t.root, None);

        t.insert(10, 11, 0); // 0
        sanity_check(&t);

        t.insert(11, 12, 0); // 1
        sanity_check(&t);

        t.insert(5, 6, 0); // 2
        sanity_check(&t);

        t.insert(7, 8, 0); // 3
        sanity_check(&t);

        t.insert(6, 7, 0); // 4
        sanity_check(&t);
    }

    #[test]
    fn rb_insert_rand_10k() {
        use rand::prelude::*;
        use rand::seq::SliceRandom;

        let mut rng = thread_rng();
        let mut xs: Vec<_> = (0..10000).collect();
        xs.shuffle(&mut rng);

        let mut t = IntervalTree::with_capacity(10000);
        for &x in &xs[..100] {
            t.insert(x, x + 1, 0);
            // println!("");
            // print_tree(&t, t.root, 0);
            sanity_check(&t);
        }

        for &x in &xs[100..10000] {
            t.insert(x, x + 1, 0);
        }
        // print_tree(&t, t.root, 0);
        sanity_check(&t);
    }

    #[bench]
    fn rb_insert_bench(b: &mut Bencher) {
        let mut t = IntervalTree::new();
        let mut i = 0;
        b.iter(|| {
            t.insert(i, i+1, 0);
            i+=1;
        });
    }

    #[bench]
    fn rb_insert_after_100k(b: &mut Bencher) {
        let mut t = IntervalTree::new();
        let mut i = 0;
        for _ in 0..100000 {
            t.insert(i, i+1, 0);
            i+=1;
        }
        b.iter(|| {
            t.insert(i, i+1, 0);
            i+=1;
        });
    }

    #[test]
    #[should_panic]
    fn rb_insert_bad() {
        let mut t = IntervalTree::new();
        t.insert_nonoverlapping(0, 1, 1);
        t.insert_nonoverlapping(0, 1, 1);
    }

    #[test]
    fn interval_find() {
        let mut t = IntervalTree::new();
        t.insert_nonoverlapping(10, 20, 0); // 0
        t.insert_nonoverlapping(20, 40, 0); // 1
        t.insert_nonoverlapping(60, 80, 0); // 2
        t.insert_nonoverlapping(80, 100, 0); // 3
        t.insert_nonoverlapping(100, 120, 0); // 4
        t.insert_nonoverlapping(140, 160, 0); // 5
        t.insert_nonoverlapping(180, 200, 0); // 6
        t.insert_nonoverlapping(220, 240, 0); // 7

        assert_eq!(t.find(0, 4), FindResult::Miss);
        assert_eq!(t.find(1, 10), FindResult::Miss);
        assert_eq!(t.find(219, 220), FindResult::Miss);

        assert_eq!(t.find(10, 20), FindResult::Equal(0));
        assert_eq!(t.find(9, 20), FindResult::Outside(0));
        assert_eq!(t.find(10, 19), FindResult::Inside(0));
        assert_eq!(t.find(21, 22), FindResult::Inside(1));
        assert_eq!(t.find(20, 22), FindResult::Inside(1));
        assert_eq!(t.find(61, 79), FindResult::Inside(2));
        assert_eq!(t.find(239, 240), FindResult::Inside(7));

        assert_eq!(t.find(9, 21), FindResult::Left(1));
        assert_eq!(t.find(219, 221), FindResult::Left(7));
        assert_eq!(t.find(239, 241), FindResult::Right(7));
    }

    #[test]
    fn interval_find_seq_1k() {
        let mut t = IntervalTree::new();

        for i in 0..1000 {
            t.insert_nonoverlapping(i, i+1, 0);
            assert_eq!(t.find(i, i+1), FindResult::Equal(i));
            assert_eq!(t.find(i+1, i+2), FindResult::Miss);
        }
    }

    #[test]
    fn rb_remove() {
        let mut t = IntervalTree::new();
        for i in 0..10 {
            t.insert_nonoverlapping(i, i+1, i);
            sanity_check(&t);
        }
        for i in 0..10 {
            let find = t.find(i, i+1);
            match find {
                FindResult::Equal(idx) => {
                    t.remove(idx);
                    sanity_check(&t);
                }
                e @ _ => { panic!("{:?}", e) }
            }
        }
    }
}
