pub mod parallel;


pub trait Executable: Send{
    fn execute(self: Box<Self>);
}

pub struct Task<T, F> where
    T: Send,
    F: FnOnce(T) + Send
{
    data: T,
    procedure: F,
}

impl<T, F> Task<T, F> where
    T: Send,
    F: FnOnce(T) + Send
{
    pub fn new(data: T, procedure: F) -> Task<T, F>{
        Task { data, procedure }
    }
}

impl<T, F> Executable for Task<T, F> where
    T: Send,
    F: FnOnce(T) + Send
{
    fn execute(self: Box<Self>) {
        (self.procedure)(self.data);
    }
}
