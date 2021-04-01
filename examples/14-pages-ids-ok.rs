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
        "IDs from first 5 pages:\n{:?}",
        get_ids_n_pages(5).collect::<Vec<_>>().await
    );
    println!(
        "IDs from first 5 pages, buffered by 3:\n{:?}",
        get_ids_n_pages_buffered(5, 3).collect::<Vec<_>>().await
    );
    println!(
        "IDs from first 5 pages, buffer-unordered by 3:\n{:?}",
        get_ids_n_pages_buffer_unordered(5, 3)
            .collect::<Vec<_>>()
            .await
    );
}

fn get_ids_n_pages(n: usize) -> impl Stream<Item = usize> {
    get_pages().take(n).flat_map(|page| stream::iter(page))
}

fn get_ids_n_pages_buffered(n: usize, buf_factor: usize) -> impl Stream<Item = usize> {
    get_pages_futures()
        .take(n)
        .buffered(buf_factor)
        .flat_map(|page| stream::iter(page))
}

fn get_ids_n_pages_buffer_unordered(n: usize, buf_factor: usize) -> impl Stream<Item = usize> {
    get_pages_futures()
        .take(n)
        .buffer_unordered(buf_factor)
        .flat_map(|page| stream::iter(page))
}

fn get_pages() -> impl Stream<Item = Vec<usize>> {
    stream::iter(0..).then(|i| get_page(i))
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

    (10 * i..10 * i + 5).collect()
}
