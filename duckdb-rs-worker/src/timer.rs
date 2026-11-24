//! One thread that runs callbacks at deadlines: statement time limits,
//! repeated interrupts and transaction deadlines (as `_Interrupter` in
//! `dblayer/backends/duckdb.py`, which avoids a thread per timer).
//!
//! Callbacks run with the timer's lock released, so they may schedule
//! again. `cancel` only stops a callback that has not started; callers
//! re-check their own state inside the callback, which is what makes a
//! late callback harmless.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

type Callback = Box<dyn FnOnce() + Send>;

#[derive(Default)]
struct State {
    heap: BinaryHeap<Reverse<(Instant, u64)>>,
    live: HashMap<u64, Callback>,
    seq: u64,
}

pub struct Timer {
    state: Mutex<State>,
    cv: Condvar,
}

impl Timer {
    pub fn start() -> Arc<Timer> {
        let t = Arc::new(Timer { state: Mutex::new(State::default()), cv: Condvar::new() });
        let t2 = Arc::clone(&t);
        // Returns once the thread runs, not merely once it is spawned: std
        // maps a thread's signal stack from inside it, and main sets
        // RLIMIT_AS right after this; a limit landing first made that
        // mmap fail and abort the worker.
        let running = Arc::new(std::sync::Barrier::new(2));
        let r2 = Arc::clone(&running);
        std::thread::Builder::new()
            .name("timer".into())
            .spawn(move || {
                r2.wait();
                t2.run()
            })
            .expect("start the timer thread");
        running.wait();
        t
    }

    pub fn schedule(&self, delay: Duration, f: impl FnOnce() + Send + 'static) -> u64 {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.seq += 1;
        let token = s.seq;
        s.heap.push(Reverse((Instant::now() + delay, token)));
        s.live.insert(token, Box::new(f));
        self.cv.notify_one();
        token
    }

    pub fn cancel(&self, token: u64) {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.live.remove(&token);
    }

    fn run(&self) {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            while let Some(Reverse((_, tok))) = s.heap.peek() {
                if s.live.contains_key(tok) {
                    break;
                }
                s.heap.pop();
            }
            let Some(&Reverse((when, tok))) = s.heap.peek() else {
                s = self.cv.wait(s).unwrap_or_else(|e| e.into_inner());
                continue;
            };
            let now = Instant::now();
            if when > now {
                s = self.cv.wait_timeout(s, when - now).unwrap_or_else(|e| e.into_inner()).0;
                continue;
            }
            s.heap.pop();
            if let Some(f) = s.live.remove(&tok) {
                drop(s);
                // A panicking callback must not stop every later timer.
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
                s = self.state.lock().unwrap_or_else(|e| e.into_inner());
            }
        }
    }
}
