use std::{
    collections::{
        hash_map::{DefaultHasher, Entry},
        HashMap,
    },
    convert::TryInto,
    error::Error,
    hash::{Hash, Hasher},
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

    fn run(mut self) -> Sender<MessageType> {
        let (pubsub_send, mut pubsub_recv) = tokio::sync::mpsc::channel(10);

        tokio::spawn(async move {
            while let Some(action) = pubsub_recv.recv().await {
                match action {
                    MessageType::Pub(topic, msg, resp) => {
                        if let Entry::Occupied(users) = self.topics.entry(topic) {
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
                        let subscribers = self.topics.entry(topic).or_default();
                        let size = subscribers.len();
                        subscribers.insert(size, sender);
                        resp.send(size).unwrap();
                    }
                    MessageType::Unsub(topic, ind, resp) => {
                        if let Entry::Occupied(mut entry) = self.topics.entry(topic) {
                            let entry = entry.get_mut();
                            entry.remove(&ind);
                        }
                        resp.send(()).unwrap();
                    }
                }
            }
        });

        pubsub_send
    }
}

struct PubSubShards<const SHARDS: usize> {
    shards: [Sender<MessageType>; SHARDS],
}

impl<const SHARDS: usize> PubSubShards<SHARDS> {
    fn new() -> PubSubShards<SHARDS> {
        let shards = (0..SHARDS)
            .map(|_| PubSub::new().run())
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();
        PubSubShards { shards }
    }

    fn run(self) -> Sender<MessageType> {
        let (send, mut rec) = tokio::sync::mpsc::channel(10);
        tokio::spawn(async move {
            while let Some(msg) = rec.recv().await {
                let topic: &str = match &msg {
                    MessageType::Pub(t, _, _) => t,
                    MessageType::Sub(t, _, _) => t,
                    MessageType::Unsub(t, _, _) => t,
                };
                let mut hasher = DefaultHasher::new();
                topic.hash(&mut hasher);
                let bucket = (hasher.finish() % SHARDS as u64) as usize;
                if let Err(err) = self.shards[bucket].send(msg).await {
                    eprintln!("{}", err.to_string());
                }
            }
        });

        send
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:8001").await?;

    let shards: PubSubShards<100> = PubSubShards::new();
    let shards = shards.run();

    loop {
        let (conn, _) = listener.accept().await?;
        let shards = shards.clone();
        handle_client_connection(conn, shards);
    }
}

fn handle_client_connection(
    mut conn: tokio::net::TcpStream,
    shards: Sender<MessageType>,
) {
    tokio::spawn(async move {
        let (conn_read, conn_write) = conn.split();
        let (conn_to_ps_send, conn_to_ps_rec): (Sender<String>, Receiver<String>) =
            mpsc::channel(10);

        let w = write_out(conn_write, conn_to_ps_rec);
        let r = read_in(conn_read, conn_to_ps_send, shards);

        futures::future::join(w, r).await;
    });
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
    shards: Sender<MessageType>,
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
                if let Err(err) = shards.send(action).await {
                    eprintln!("pub send error - {}", err.to_string())
                }
                if let Err(err) = done.await {
                    eprintln!("pub oneshot error - {}", err.to_string());
                }
            }
            "s" | "sub" | "subscribe" if parts.len() == 2 => {
                let (notify_done, done) = oneshot::channel();
                let action = MessageType::Sub(parts[1].to_string(), conn_to_ps_send.clone(), notify_done);
                if let Err(err) = shards.send(action).await {
                    eprintln!("sub send error - {}", err.to_string())
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
                let ind = match positions.get(&parts[1]) {
                    Some(&ind) => ind,
                    None => continue
                };
                let (notify_done, done) = oneshot::channel();
                let action = MessageType::Unsub(parts[1].clone(), ind, notify_done);
                if let Err(err) = shards.send(action).await {
                    eprintln!("pub send error - {}", err.to_string())
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
        if let Err(err) = shards.send(action).await {
            eprintln!("pub send error - {}", err.to_string())
        }
        if let Err(err) = or.await {
            eprintln!("unsub oneshot error - {}", err.to_string());
        }
    }
}
