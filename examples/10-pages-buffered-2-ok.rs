use futures::{stream, Future, Stream, StreamExt};
use lazy_static::lazy_static;
use rand::distributions::{Distribution, Uniform};
use std::time::Duration;
use tokio::time::{sleep, Instant};

lazy_static! {
    static ref START_TIME: Instant = Instant::now();
}

#[tokio::main]
async fn main() {
    println!(
        "First 10 pages, buffered by 5:\n{:?}",
        get_n_pages_buffered(10, 5).await
    );
}

async fn get_n_pages_buffered(n: usize, buf_factor: usize) -> Vec<Vec<usize>> {
    get_pages_buffered(buf_factor).take(n).collect().await
}

fn get_pages_buffered(buf_factor: usize) -> impl Stream<Item = Vec<usize>> {
    get_pages_futures().buffered(buf_factor)
}

fn get_pages_futures() -> impl Stream<Item = impl Future<Output = Vec<usize>>> {
    stream::iter(0..).map(|i| get_page(i))
}

async fn get_page(i: usize) -> Vec<usize> {
    let millis = Uniform::from(0..10).sample(&mut rand::thread_rng());
    println!(
        "[{}] # get_page({}) will complete in {} ms",
        START_TIME.elapsed().as_millis(),
        i,
        millis
    );

    sleep(Duration::from_millis(millis)).await;
    println!(
        "[{}] # get_page({}) completed",
        START_TIME.elapsed().as_millis(),
        i
    );

    (10 * i..10 * (i + 1)).collect()
}
