use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::join;
use futures::stream::StreamExt;
use tokio::spawn;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Send 10 queries");
    send_receive(10).await?;
    Ok(())
}

async fn send_receive(n: usize) -> Result<(), Box<dyn std::error::Error>> {
    let (tx, rx) = unbounded();

    let send = spawn(async move {
        send_task(tx, n).await;
    });

    let receive = spawn(async move {
        receive_task(rx).await;
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

async fn receive_task(rx: UnboundedReceiver<usize>) {
    rx.for_each(|i| async move { println!("# query({})", i) })
        .await;
}
