use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::join;
use futures::stream::StreamExt;
use lazy_static::lazy_static;
use rand::distributions::{Distribution, Uniform};
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::spawn;
use tokio::time::{sleep, Instant};

lazy_static! {
    static ref START_TIME: Instant = Instant::now();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Cancel 25 queries, buffered by 3");
    cancel_queries_buffered(5, 3).await?;
    Ok(())
}

async fn cancel_queries_buffered(
    n: usize,
    buf_factor: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let (tx, rx) = unbounded();
    let (valid_writer, valid_reader) = ValidRange::new();
    let counter = Arc::new(ValidCounter::new());

    let send = spawn(async move {
        send_task_tracking_validity(tx, n, valid_writer).await;
    });

    let counter_writer = counter.clone();
    let receive = spawn(async move {
        receive_task_buffered_cancelling(rx, buf_factor, &valid_reader, &counter_writer).await;
    });

    let (send_res, receive_res) = join!(send, receive);
    send_res?;
    receive_res?;

    counter.print();
    Ok(())
}

async fn send_task_tracking_validity(
    tx: UnboundedSender<usize>,
    n: usize,
    valid_writer: ValidRange,
) {
    for i in 0..n {
        let range = 10 * i..10 * i + 5;
        valid_writer.set(range.clone());
        for j in range {
            println!("## unbounded_send({})", j);
            tx.unbounded_send(j).unwrap();
        }
        let millis = Uniform::from(0..10).sample(&mut rand::thread_rng());
        println!("## sleep({}) for {} ms", i, millis);

        let duration = Duration::from_millis(millis);
        sleep(duration).await;
        println!("## sleep({}) completed", i);
    }
}

async fn receive_task_buffered_cancelling(
    rx: UnboundedReceiver<usize>,
    buf_factor: usize,
    valid_reader: &ValidRange,
    counter_writer: &Arc<ValidCounter>,
) {
    rx.filter(|i| {
        let is_valid = valid_reader.is_valid(*i);
        println!("## filter({}) = {}", i, is_valid);
        async move { is_valid }
    })
    .map(|i| get_data(i))
    .buffered(buf_factor)
    .for_each(|data| async move {
        let is_valid = valid_reader.is_valid(data.0);
        counter_writer.increment(is_valid);
        println!(
            "## data = {:?} ({})",
            data,
            if is_valid { "valid" } else { "expired" }
        );
    })
    .await;
}

#[derive(Clone)]
struct ValidRange {
    range: Arc<RwLock<Range<usize>>>,
}

impl ValidRange {
    fn new() -> (ValidRange, ValidRange) {
        let writer = Arc::new(RwLock::new(0..0));
        let reader = writer.clone();
        (ValidRange { range: writer }, ValidRange { range: reader })
    }

    fn set(&self, range: Range<usize>) {
        *self.range.write().unwrap() = range;
    }

    fn is_valid(&self, x: usize) -> bool {
        self.range.read().unwrap().contains(&x)
    }
}

struct ValidCounter {
    valid: AtomicUsize,
    expired: AtomicUsize,
}

impl ValidCounter {
    fn new() -> ValidCounter {
        ValidCounter {
            valid: AtomicUsize::new(0),
            expired: AtomicUsize::new(0),
        }
    }

    fn increment(&self, is_valid: bool) {
        if is_valid {
            self.valid.fetch_add(1, Ordering::SeqCst);
        } else {
            self.expired.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn print(&self) {
        let valid = self.valid.load(Ordering::SeqCst);
        let expired = self.expired.load(Ordering::SeqCst);

        println!(
            "Made {} queries, {} results were still valid, {} expired",
            valid + expired,
            valid,
            expired
        );
    }
}

#[derive(Clone, Copy)]
struct Data(usize);

impl std::fmt::Debug for Data {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("d:{}", self.0))
    }
}

async fn get_data(i: usize) -> Data {
    let millis = Uniform::from(0..10).sample(&mut rand::thread_rng());
    println!(
        "[{}] ## get_data({}) will complete in {} ms",
        START_TIME.elapsed().as_millis(),
        i,
        millis
    );

    sleep(Duration::from_millis(millis)).await;
    println!(
        "[{}] ## get_data({}) completed",
        START_TIME.elapsed().as_millis(),
        i
    );
    Data(i)
}
