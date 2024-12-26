macro_rules! receiver_iters {
    () => {
        impl<T> Receiver<T> {
            pub fn iter(&self) -> Iter<'_, T> {
                Iter { rx: self }
            }

            pub fn try_iter(&self) -> TryIter<'_, T> {
                TryIter { rx: self }
            }
        }

        pub struct Iter<'a, T> {
            rx: &'a Receiver<T>,
        }

        impl<'a, T> Iterator for Iter<'a, T> {
            type Item = T;

            fn next(&mut self) -> Option<T> {
                self.rx.recv().ok()
            }
        }

        pub struct TryIter<'a, T> {
            rx: &'a Receiver<T>,
        }

        impl<'a, T> Iterator for TryIter<'a, T> {
            type Item = T;

            fn next(&mut self) -> Option<T> {
                self.rx.try_recv().ok()
            }
        }

        pub struct IntoIter<T> {
            rx: Receiver<T>,
        }

        impl<T> Iterator for IntoIter<T> {
            type Item = T;

            fn next(&mut self) -> Option<T> {
                self.rx.recv().ok()
            }
        }

        impl<T> IntoIterator for Receiver<T> {
            type Item = T;
            type IntoIter = IntoIter<T>;

            fn into_iter(self) -> IntoIter<T> {
                IntoIter { rx: self }
            }
        }

        impl<'a, T> IntoIterator for &'a Receiver<T> {
            type Item = T;
            type IntoIter = Iter<'a, T>;

            fn into_iter(self) -> Iter<'a, T> {
                self.iter()
            }
        }
    };
}

mod error;
pub mod mpmc;
pub mod spsc;
mod wait;

pub use error::{
    RecvError, RecvTimeoutError, SendError, SendTimeoutError, TryRecvError, TrySendError,
};
