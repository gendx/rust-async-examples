use futures::{stream, StreamExt};
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
        "Resources from first 5 pages:\n{:?}",
        collect_resources_n_pages(5).await
    );
    println!(
        "Resources from first 5 pages, buffered by 3:\n{:?}",
        collect_resources_n_pages_buffered(5, 3).await
    );
    println!(
        "Resources from first 5 pages, buffer-unordered by 3:\n{:?}",
        collect_resources_n_pages_buffer_unordered(5, 3).await
    );
}

async fn collect_resources_n_pages(n: usize) -> Vec<Resource> {
    stream::iter(0..)
        .then(|i| get_page(i))
        .take(n)
        .flat_map(|page| stream::iter(page))
        .then(|id| fetch_resource(id))
        .collect()
        .await
}

async fn collect_resources_n_pages_buffered(n: usize, buf_factor: usize) -> Vec<Resource> {
    stream::iter(0..)
        .map(|i| get_page(i))
        .take(n)
        .buffered(buf_factor)
        .flat_map(|page| stream::iter(page))
        .map(|id| fetch_resource(id))
        .buffered(buf_factor)
        .collect()
        .await
}

async fn collect_resources_n_pages_buffer_unordered(n: usize, buf_factor: usize) -> Vec<Resource> {
    stream::iter(0..)
        .map(|i| get_page(i))
        .take(n)
        .buffer_unordered(buf_factor)
        .flat_map(|page| stream::iter(page))
        .map(|id| fetch_resource(id))
        .buffer_unordered(buf_factor)
        .collect()
        .await
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

#[derive(Clone, Copy)]
struct Resource(usize);

impl std::fmt::Debug for Resource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("r:{}", self.0))
    }
}

async fn fetch_resource(i: usize) -> Resource {
    let millis = Uniform::from(0..10).sample(&mut rand::thread_rng());
    println!(
        "[{}] ## fetch_resource({}) will complete in {} ms",
        START_TIME.elapsed().as_millis(),
        i,
        millis
    );

    sleep(Duration::from_millis(millis)).await;
    println!(
        "[{}] ## fetch_resource({}) completed",
        START_TIME.elapsed().as_millis(),
        i
    );
    Resource(i)
}
