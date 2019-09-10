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

/// This trait defines a merge operator, which is used to merge a pair
/// of values of the same type into a single value.
pub trait Merge {
    fn merge(self, rhs: Self) -> Self;
}

/// This trait defines a split operator, which is used to split a
/// value into a pair of values of the same type.
pub trait Split: Sized {
    type Position;

    fn split(self, pos: Self::Position) -> (Self, Self);
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum Color {
    Red,
    Black,
}
use Color::*;

pub type Index = usize;

pub enum FindResult {
    Hit(Index),
    Miss,
    Partial(Index),
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
    V: Clone,
{
    root: Option<Index>,
    tree: Slab<Node<K, V>>,
}

/// A TreeNode represents an interval [a, b).
#[derive(Clone)]
struct Node<K, V>
where
    K: Eq + Ord + Copy,
    V: Clone,
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
    V: Clone,
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

    /// Insert a new interval; if there's an overlapping, merge with
    /// it.
    pub fn insert(&mut self, a: K, b: K, value: V) {
        self.insert_nonoverlapping(a, b, value)
    }

    /// Insert a new interval into this tree.
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

    pub fn remove(&mut self) {
        unimplemented!()
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

    pub fn find(&self, a: K, b: K) -> FindResult {
        let maybe = self.root()
            .map(|(id, x)| x.find(id, &self.tree, a, b));
        match maybe {
            Some(result) => result,
            None => FindResult::Miss,
        }
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
        &self.tree[id as usize]
    }

    fn node_mut(&mut self, id: Index) -> &mut Node<K,V> {
        &mut self.tree[id as usize]
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
    V: Clone,
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
    where K: Ord + Copy + Eq, V: Clone,
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

        t.insert(10, 100, 0); // 0
        sanity_check(&t);

        t.insert(11, 100, 0); // 1
        sanity_check(&t);

        t.insert(6, 100, 0); // 2
        sanity_check(&t);

        t.insert(4, 100, 0); // 3
        sanity_check(&t);

        t.insert(2, 100, 0); // 4
        sanity_check(&t);
    }

    #[test]
    fn rb_insert1() {
        let mut t = IntervalTree::new();
        assert_eq!(t.root, None);

        t.insert(10, 100, 0); // 0
        sanity_check(&t);

        t.insert(11, 100, 0); // 1
        sanity_check(&t);

        t.insert(5, 100, 0); // 2
        sanity_check(&t);

        t.insert(7, 100, 0); // 3
        sanity_check(&t);

        t.insert(6, 100, 0); // 4
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
            println!("");
            print_tree(&t, t.root, 0);
            sanity_check(&t);
        }

        for &x in &xs[100..10000] {
            t.insert(x, x + 1, 0);
        }
        print_tree(&t, t.root, 0);
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
}


