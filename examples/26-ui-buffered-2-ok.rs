use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::join;
use futures::stream::StreamExt;
use lazy_static::lazy_static;
use rand::distributions::{Distribution, Uniform};
use std::time::Duration;
use tokio::spawn;
use tokio::time::{sleep, Instant};

lazy_static! {
    static ref START_TIME: Instant = Instant::now();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Resolve 10 queries, buffered by 5");
    send_receive_queries_buffered(10, 5).await?;
    Ok(())
}

async fn send_receive_queries_buffered(
    n: usize,
    buf_factor: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let (tx, rx) = unbounded();

    let send = spawn(async move {
        send_task(tx, n).await;
    });

    let receive = spawn(async move {
        receive_task_queries_buffered(rx, buf_factor).await;
    });

    let (send_res, receive_res) = join!(send, receive);
    send_res?;
    receive_res?;
    Ok(())
}

async fn send_task(tx: UnboundedSender<usize>, n: usize) {
    for i in 0..n {
        tx.unbounded_send(i).unwrap();
    }
}

async fn receive_task_queries_buffered(rx: UnboundedReceiver<usize>, buf_factor: usize) {
    rx.map(|i| get_data(i))
        .buffered(buf_factor)
        .for_each(|data| async move {
            println!("## data = {:?}", data);
        })
        .await;
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
