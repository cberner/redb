use std::rc::Rc;

pub trait Operator1<T> {
    fn execute(&self) -> (Rc<Vec<T>>,);
}

pub trait Operator2<T, U> {
    fn execute(&self) -> (Rc<Vec<T>>, Rc<Vec<U>>);
}

pub trait Operator3<T, U, V> {
    fn execute(&self) -> (Rc<Vec<T>>, Rc<Vec<U>>, Rc<Vec<V>>);
}

pub trait Operator4<T, U, V, W> {
    fn execute(&self) -> (Rc<Vec<T>>, Rc<Vec<U>>, Rc<Vec<V>>, Rc<Vec<W>>);
}
