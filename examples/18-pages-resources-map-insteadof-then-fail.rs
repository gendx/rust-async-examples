use futures::{stream, Stream, StreamExt};
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
}

async fn collect_resources_n_pages(n: usize) -> Vec<Resource> {
    get_ids_n_pages(n)
        .map(|id| fetch_resource(id))
        .collect()
        .await
}

fn get_ids_n_pages(n: usize) -> impl Stream<Item = usize> {
    get_pages().take(n).flat_map(|page| stream::iter(page))
}

fn get_pages() -> impl Stream<Item = Vec<usize>> {
    stream::iter(0..).then(|i| get_page(i))
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
