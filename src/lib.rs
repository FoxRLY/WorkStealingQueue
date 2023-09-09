pub mod parallel;

pub trait Executable: Send {
    fn execute(self: Box<Self>);
}

pub struct Task<T>
where
    T: FnOnce() + Send,
{
    procedure: T,
}

impl<T> Task<T>
where
    T: FnOnce() + Send,
{
    pub fn new(procedure: T) -> Task<T> {
        Task { procedure }
    }
}

impl<T> Executable for Task<T>
where
    T: FnOnce() + Send,
{
    fn execute(self: Box<Self>) {
        (self.procedure)();
    }
}
