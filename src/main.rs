use std::{
    collections::{hash_map::Entry, HashMap},
    error::Error,
};

use tokio::{
    self,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{
        tcp::{ReadHalf, WriteHalf},
        TcpListener,
    },
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
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
                    let ent = pubsub.topics.entry(topic).or_default();
                    let size = ent.len();
                    ent.insert(size, sender);
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
            let (mut conn_read, mut conn_write) = conn.split();
            let (conn_to_ps_send, conn_to_ps_rec): (Sender<String>, Receiver<String>) =
                mpsc::channel(10);
            let (closer_send, closer_recv): (
                tokio::sync::oneshot::Sender<()>,
                tokio::sync::oneshot::Receiver<()>,
            ) = tokio::sync::oneshot::channel();

            let w = write_out(&mut conn_write, conn_to_ps_rec, closer_send);
            let r = read_in(&mut conn_read, conn_to_ps_send, pubsub_send, closer_recv);

            futures::future::join(w, r).await;
        });
    }
}

async fn write_out(
    conn_write: &mut WriteHalf<'_>,
    mut conn_to_ps_rec: Receiver<String>,
    mut closer: tokio::sync::oneshot::Sender<()>,
) {
    loop {
        tokio::select! {
            msg = conn_to_ps_rec.recv() => {
                if let Some(mut msg) = msg {
                    msg.push('\n');
                    if let Err(err) = conn_write.write(msg.as_bytes()).await {
                        eprintln!("write out error - {}", err.to_string());
                    }
                }
            }
            _ = closer.closed() => return
        }
    }
}

async fn read_in(
    conn_read: &mut ReadHalf<'_>,
    conn_to_ps_send: Sender<String>,
    pubsub_send: Sender<MessageType>,
    mut closer: tokio::sync::oneshot::Receiver<()>,
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
                let (os, or) = oneshot::channel();
                let action = MessageType::Pub(parts[1].clone(), parts[2].clone(), os);
                if let Err(err) = pubsub_send.send(action).await {
                    eprintln!("pub send error - {}", err.to_string());
                }
                if let Err(err) = or.await {
                    eprintln!("pub oneshot error - {}", err.to_string());
                }
            }
            "s" | "sub" | "subscribe" if parts.len() == 2 => {
                let (os, or) = oneshot::channel();
                let action = MessageType::Sub(parts[1].to_string(), conn_to_ps_send.clone(), os);
                if let Err(err) = pubsub_send.send(action).await {
                    eprintln!("set send error - {}", err.to_string());
                }
                let ind = match or.await {
                    Ok(ind) => ind,
                    Err(err) => {
                        eprintln!("set oneshot error - {}", err.to_string());
                        continue;
                    }
                };
                positions.insert(parts[1].clone(), ind);
            }
            "u" | "unsub" | "unsubscribe" if parts.len() == 2 => {
                let (os, or) = oneshot::channel();
                let ind = *positions.get(&parts[1]).unwrap();
                let action = MessageType::Unsub(parts[1].clone(), ind, os);
                if let Err(err) = pubsub_send.send(action).await {
                    eprintln!("uns send error - {}", err.to_string())
                }
                if let Err(err) = or.await {
                    eprintln!("ubs oneshot error - {}", err.to_string());
                }
                positions.remove(&parts[1]);
            }
            _ => {
                if let Err(err) = conn_to_ps_send.send("bad input command, please pub `p subject message`, sub `s topic` or unsub `u topic`".into()).await {
                    eprintln!("uns send error - {}", err.to_string())
                }
            }
        }
    }

    for (topic, ind) in positions {
        let (os, or) = oneshot::channel();
        let action = MessageType::Unsub(topic, ind, os);
        if let Err(err) = pubsub_send.send(action).await {
            eprintln!("uns send error - {}", err.to_string())
        }
        if let Err(err) = or.await {
            eprintln!("ubs oneshot error - {}", err.to_string());
        }
    }

    closer.close();
}
