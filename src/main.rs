use std::{
    collections::{hash_map::Entry, HashMap},
    error::Error,
};

use tokio::{
    self,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{
        tcp::{ReadHalf, WriteHalf},
        TcpListener,
    },
    select,
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
    time::{Duration, Instant},
};

enum MessageType {
    Pub(String, String, oneshot::Sender<()>),
    Sub(String, Sender<String>, oneshot::Sender<usize>),
    Unsub(String, usize, oneshot::Sender<()>),
}

struct PubSub {
    topics: HashMap<String, HashMap<usize, Sender<String>>>,
}

impl PubSub {
    fn new() -> PubSub {
        PubSub {
            topics: HashMap::new(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:8001").await?;

    let mut pubsub = PubSub::new();
    let (pubsub_send, mut pubsub_recv) = tokio::sync::mpsc::channel(10);

    tokio::spawn(async move {
        while let Some(action) = pubsub_recv.recv().await {
            match action {
                MessageType::Pub(topic, msg, resp) => {
                    if let Entry::Occupied(users) = pubsub.topics.entry(topic) {
                        let users = users
                            .get()
                            .values()
                            .map(|s| s.send(msg.clone()))
                            .collect::<Vec<_>>();
                        futures::future::join_all(users).await;
                    }
                    resp.send(()).unwrap();
                }
                MessageType::Sub(topic, sender, resp) => {
                    let subscribers = pubsub.topics.entry(topic).or_default();
                    let size = subscribers.len();
                    subscribers.insert(size, sender);
                    resp.send(size).unwrap();
                }
                MessageType::Unsub(topic, ind, resp) => {
                    if let Entry::Occupied(mut entry) = pubsub.topics.entry(topic) {
                        let entry = entry.get_mut();
                        entry.remove(&ind);
                    }
                    resp.send(()).unwrap();
                }
            }
        }
    });

    loop {
        let (mut conn, _) = listener.accept().await?;
        let pubsub_send = pubsub_send.clone();

        tokio::spawn(async move {
            let (conn_read, conn_write) = conn.split();
            let (conn_to_ps_send, conn_to_ps_rec): (Sender<String>, Receiver<String>) =
                mpsc::channel(10);

            let w = write_out(conn_write, conn_to_ps_rec);
            let r = read_in(conn_read, conn_to_ps_send, pubsub_send);

            futures::future::join(w, r).await;
        });
    }
}

async fn write_out(conn_write: WriteHalf<'_>, mut conn_to_ps_rec: Receiver<String>) {
    let mut writer = BufWriter::new(conn_write);
    let mut limit = Instant::now() + Duration::from_millis(500);

    loop {
        select! {
            msg = conn_to_ps_rec.recv() => {
                if let Some(mut msg) = msg {
                    msg.push('\n');
                    if let Err(err) = writer.write(msg.as_bytes()).await {
                        eprintln!("write out error - {}", err.to_string());
                    }
                } else {
                    if let Err(err) = writer.flush().await {
                        eprintln!("{}", err.to_string());
                    }
                    return;
                }
            }
            _ = tokio::time::sleep_until(limit) => {
                if let Err(err) = writer.flush().await {
                    eprintln!("{}", err.to_string());
                }
                limit += Duration::from_millis(500);
            }
        }
    }
}

async fn read_in(
    conn_read: ReadHalf<'_>,
    conn_to_ps_send: Sender<String>,
    pubsub_send: Sender<MessageType>,
) {
    let mut user_input = String::new();
    let mut reader = BufReader::new(conn_read);

    let mut positions: HashMap<String, usize> = HashMap::new();

    while let Ok(read) = reader.read_line(&mut user_input).await {
        if read == 0 {
            break;
        }

        user_input.pop();
        if user_input.is_empty() {
            continue;
        }

        let parts = user_input
            .split(' ')
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        user_input.clear();

        match parts[0].as_str() {
            "p" | "pub" | "publish" if parts.len() == 3 => {
                let (notify_done, done) = oneshot::channel();
                let action = MessageType::Pub(parts[1].clone(), parts[2].clone(), notify_done);
                if let Err(err) = pubsub_send.send(action).await {
                    eprintln!("pub send error - {}", err.to_string());
                }
                if let Err(err) = done.await {
                    eprintln!("pub oneshot error - {}", err.to_string());
                }
            }
            "s" | "sub" | "subscribe" if parts.len() == 2 => {
                let (notify_done, done) = oneshot::channel();
                let action = MessageType::Sub(parts[1].to_string(), conn_to_ps_send.clone(), notify_done);
                if let Err(err) = pubsub_send.send(action).await {
                    eprintln!("set send error - {}", err.to_string());
                }
                let ind = match done.await {
                    Ok(ind) => ind,
                    Err(err) => {
                        eprintln!("set oneshot error - {}", err.to_string());
                        continue;
                    }
                };
                positions.insert(parts[1].clone(), ind);
            }
            "u" | "unsub" | "unsubscribe" if parts.len() == 2 => {
                let (notify_done, done) = oneshot::channel();
                let ind = match positions.get(&parts[1]) {
                    Some(&ind) => ind,
                    None => continue
                };
                let action = MessageType::Unsub(parts[1].clone(), ind, notify_done);
                if let Err(err) = pubsub_send.send(action).await {
                    eprintln!("unsub send error - {}", err.to_string())
                }
                if let Err(err) = done.await {
                    eprintln!("unsub oneshot error - {}", err.to_string());
                }
                positions.remove(&parts[1]);
            }
            _ => {
                if let Err(err) = conn_to_ps_send.send("bad input command, please pub `p subject message`, sub `s topic` or unsub `u topic`".into()).await {
                    eprintln!("unsub send error - {}", err.to_string())
                }
            }
        }
    }

    for (topic, ind) in positions {
        let (os, or) = oneshot::channel();
        let action = MessageType::Unsub(topic, ind, os);
        if let Err(err) = pubsub_send.send(action).await {
            eprintln!("unsub send error - {}", err.to_string())
        }
        if let Err(err) = or.await {
            eprintln!("unsub oneshot error - {}", err.to_string());
        }
    }
}
