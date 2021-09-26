//! Thread pool that joins all thread when dropped.

#![allow(clippy::mutex_atomic)]

// NOTE: Crossbeam channels are MPMC, which means that you don't need to wrap the receiver in
// Arc<Mutex<..>>. Just clone the receiver and give it to each worker thread.
use crossbeam_channel::{unbounded, Sender, RecvError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use itertools::{join, Itertools};

struct Job(Box<dyn FnOnce() + Send + 'static>);

#[derive(Debug)]
struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Drop for Worker {
    /// When dropped, the thread's `JoinHandle` must be `join`ed.  If the worker panics, then this
    /// function should panic too.  NOTE: that the thread is detached if not `join`ed explicitly.
    fn drop(&mut self) {
        self.thread.take().unwrap().join();
    }
}

/// Internal data structure for tracking the current job status. This is shared by the worker
/// closures via `Arc` so that the workers can report to the pool that it started/finished a job.
#[derive(Debug, Default)]
struct ThreadPoolInner {
    job_count: Mutex<usize>,
    empty_condvar: Condvar,
}

impl ThreadPoolInner {
    /// Increment the job count.
    fn start_job(&self) {
        *self.job_count.lock().unwrap() += 1;
    }

    /// Decrement the job count.
    fn finish_job(&self) {
        *self.job_count.lock().unwrap() -= 1;
        self.empty_condvar.notify_all();
    }

    /// Wait until the job count becomes 0.
    ///
    /// NOTE: We can optimize this function by adding another field to `ThreadPoolInner`, but let's
    /// not care about that in this homework.
    fn wait_empty(&self) {
        let l = self.job_count.lock().unwrap();
        self.empty_condvar.wait_while(l, |a| { *a > 0usize });
    }
}

/// Thread pool.
#[derive(Debug)]
pub struct ThreadPool {
    workers: Vec<Worker>,
    job_sender: Option<crossbeam_channel::Sender<Job>>,
    pool_inner: Arc<ThreadPoolInner>,
}

impl ThreadPool {
    /// Create a new ThreadPool with `size` threads. Panics if the size is 0.
    pub fn new(size: usize) -> Self {
        assert!(size > 0);

        let (sender, receiver) = crossbeam_channel::unbounded::<Job>();

        let inner_pool = Arc::new(ThreadPoolInner::default());
        ThreadPool {
            workers: (0..size).map(|id| {
                let receiver = receiver.clone();
                let inner_pool = inner_pool.clone();
                Worker {
                    id,
                    thread: Some(thread::spawn(move || loop {
                        let job = receiver.recv();

                        match job {
                            Ok(f) => {
                                inner_pool.start_job();

                                println!("Worker {} got a job; executing.", id);
                                (f.0)();
                                inner_pool.finish_job()
                            }
                            Err(_) => {
                                println!("Worker {} was told to terminate.", id);
                                break;
                            }
                        }
                    })),
                }
            }).collect_vec(),
            job_sender: Some(sender),
            pool_inner: inner_pool,
        }
    }

    /// Execute a new job in the thread pool.
    pub fn execute<F>(&self, f: F)
        where
            F: FnOnce() + Send + 'static,
    {
        let job = Job{0: Box::new(f)};

        self.job_sender.as_ref().unwrap().send(job).unwrap()
    }

    /// Block the current thread until all jobs in the pool have been executed.  NOTE: This method
    /// has nothing to do with `JoinHandle::join`.
    pub fn join(&self) {
        self.pool_inner.wait_empty();
    }
}

impl Drop for ThreadPool {
    /// When dropped, all worker threads' `JoinHandle` must be `join`ed. If the thread panicked,
    /// then this function should panic too.
    fn drop(&mut self) {
        self.job_sender.take();
    }
}
