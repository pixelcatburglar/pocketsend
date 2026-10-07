use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};

use crate::ftp::{Client, ConnectionSettings, Entry};

pub enum Command {
    Connect {
        settings: ConnectionSettings,
        path: String,
    },
    List {
        path: String,
    },
    CreateDir {
        path: String,
    },
    Upload {
        id: u64,
        local: PathBuf,
        remote: String,
    },
    Disconnect,
    Shutdown,
}

pub enum Event {
    Connected { path: String, entries: Vec<Entry> },
    Listed { path: String, entries: Vec<Entry> },
    DirectoryCreated { path: String },
    Disconnected,
    Progress { id: u64, done: u64, total: u64 },
    Uploaded { id: u64 },
    Failed { id: Option<u64>, message: String },
}

/// One thread owns the FTP connection, keeping all network I/O off the UI thread.
pub struct Worker {
    pub commands: Sender<Command>,
    pub events: Receiver<Event>,
    /// The caller clears this immediately before submitting a new upload.
    pub cancel: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
}

impl Worker {
    pub fn spawn(repaint: impl Fn() + Send + Sync + 'static) -> Self {
        Self::spawn_with_connector(repaint, Client::connect)
    }

    fn spawn_with_connector<C, F>(repaint: impl Fn() + Send + Sync + 'static, connector: F) -> Self
    where
        C: Connection + Send + 'static,
        F: FnMut(&ConnectionSettings) -> Result<C> + Send + 'static,
    {
        let (commands, command_rx) = mpsc::channel();
        let (event_tx, events) = mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker_cancel = Arc::clone(&cancel);
        let worker_shutdown = Arc::clone(&shutdown);

        thread::Builder::new()
            .name("ftp-worker".to_owned())
            .spawn(move || {
                run(
                    command_rx,
                    |event| {
                        if event_tx.send(event).is_ok() {
                            repaint();
                        }
                    },
                    &worker_cancel,
                    &worker_shutdown,
                    connector,
                );
            })
            .expect("Could not start the FTP worker thread");

        Self {
            commands,
            events,
            cancel,
            shutdown,
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.cancel.store(true, Ordering::Relaxed);
        // Wake an idle worker. Never wait for a socket timeout on the UI thread.
        let _ = self.commands.send(Command::Shutdown);
    }
}

// This small boundary also lets the queue behavior be tested without a network.
trait Connection {
    fn list(&mut self, path: &str) -> Result<Vec<Entry>>;
    fn create_dir(&mut self, path: &str) -> Result<()>;
    fn upload(
        &mut self,
        local: &Path,
        remote: &str,
        cancel: &AtomicBool,
        progress: &mut dyn FnMut(u64, u64),
    ) -> Result<()>;
    fn disconnect(self);
}

impl Connection for Client {
    fn list(&mut self, path: &str) -> Result<Vec<Entry>> {
        Client::list(self, path)
    }

    fn create_dir(&mut self, path: &str) -> Result<()> {
        Client::create_dir(self, path)
    }

    fn upload(
        &mut self,
        local: &Path,
        remote: &str,
        cancel: &AtomicBool,
        progress: &mut dyn FnMut(u64, u64),
    ) -> Result<()> {
        Client::upload(self, local, remote, cancel, progress)
    }

    fn disconnect(self) {
        Client::disconnect(self);
    }
}

fn ensure_connection<'a, C: Connection>(
    connection: &'a mut Option<C>,
    settings: &Option<ConnectionSettings>,
    connector: &mut impl FnMut(&ConnectionSettings) -> Result<C>,
) -> Result<&'a mut C> {
    if connection.is_none() {
        let settings = settings.as_ref().context("Connect to your 3DS first")?;
        *connection = Some(connector(settings).context("Could not connect to the 3DS")?);
    }
    Ok(connection
        .as_mut()
        .expect("Connection was just established"))
}

fn run<C: Connection>(
    commands: Receiver<Command>,
    emit: impl Fn(Event),
    cancel: &AtomicBool,
    shutdown: &AtomicBool,
    mut connector: impl FnMut(&ConnectionSettings) -> Result<C>,
) {
    let mut connection: Option<C> = None;
    let mut settings = None;

    while let Ok(command) = commands.recv() {
        if shutdown.load(Ordering::Relaxed) || matches!(command, Command::Shutdown) {
            break;
        }

        let id = match &command {
            Command::Upload { id, .. } => Some(*id),
            _ => None,
        };

        let result = (|| -> Result<()> {
            match command {
                Command::Connect {
                    settings: new_settings,
                    path,
                } => {
                    // A new connection request replaces the previous session.
                    connection = None;
                    settings = Some(new_settings);
                    let client = ensure_connection(&mut connection, &settings, &mut connector)?;
                    let entries = client
                        .list(&path)
                        .with_context(|| format!("Could not open {path}"))?;
                    emit(Event::Connected { path, entries });
                }
                Command::List { path } => {
                    let client = ensure_connection(&mut connection, &settings, &mut connector)?;
                    let entries = client
                        .list(&path)
                        .with_context(|| format!("Could not open {path}"))?;
                    emit(Event::Listed { path, entries });
                }
                Command::CreateDir { path } => {
                    let client = ensure_connection(&mut connection, &settings, &mut connector)?;
                    client
                        .create_dir(&path)
                        .with_context(|| format!("Could not create folder {path}"))?;
                    emit(Event::DirectoryCreated { path });
                }
                Command::Upload { id, local, remote } => {
                    // Do not clear this here: cancellation can race a queued command.
                    if cancel.load(Ordering::Relaxed) {
                        bail!("Transfer cancelled");
                    }
                    let client = ensure_connection(&mut connection, &settings, &mut connector)?;
                    let mut last_update: Option<Instant> = None;
                    client
                        .upload(&local, &remote, cancel, &mut |done, total| {
                            let now = Instant::now();
                            if done == total
                                || last_update.is_none_or(|last| {
                                    now.duration_since(last) >= Duration::from_millis(100)
                                })
                            {
                                last_update = Some(now);
                                emit(Event::Progress { id, done, total });
                            }
                        })
                        .with_context(|| {
                            format!("Could not send {} to {remote}", local.display())
                        })?;
                    emit(Event::Uploaded { id });
                }
                Command::Disconnect => {
                    settings = None;
                    if let Some(client) = connection.take() {
                        client.disconnect();
                    }
                    emit(Event::Disconnected);
                }
                Command::Shutdown => unreachable!(),
            }
            Ok(())
        })();

        if let Err(error) = result {
            // A failed transfer can leave unread protocol responses behind. Drop
            // the socket immediately; reconnect only for a future command.
            connection = None;
            let message = if id.is_some() && cancel.load(Ordering::Relaxed) {
                "Transfer cancelled".to_owned()
            } else {
                format!("{error:#}")
            };
            emit(Event::Failed { id, message });
            emit(Event::Disconnected);
        }
    }
    // Dropping the socket avoids delaying process shutdown with a QUIT exchange.
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, atomic::AtomicUsize};

    #[derive(Default)]
    struct State {
        connects: AtomicUsize,
        uploads: AtomicUsize,
        drops: AtomicUsize,
        fail_upload: AtomicBool,
        fail_list: AtomicBool,
        operations: Mutex<Vec<String>>,
    }

    struct FakeConnection(Arc<State>);

    impl Drop for FakeConnection {
        fn drop(&mut self) {
            self.0.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl Connection for FakeConnection {
        fn list(&mut self, path: &str) -> Result<Vec<Entry>> {
            self.0
                .operations
                .lock()
                .unwrap()
                .push(format!("list {path}"));
            if self.0.fail_list.swap(false, Ordering::Relaxed) {
                bail!("listing failed");
            }
            Ok(Vec::new())
        }

        fn create_dir(&mut self, path: &str) -> Result<()> {
            self.0
                .operations
                .lock()
                .unwrap()
                .push(format!("mkdir {path}"));
            Ok(())
        }

        fn upload(
            &mut self,
            _local: &Path,
            remote: &str,
            cancel: &AtomicBool,
            progress: &mut dyn FnMut(u64, u64),
        ) -> Result<()> {
            self.0.uploads.fetch_add(1, Ordering::Relaxed);
            self.0
                .operations
                .lock()
                .unwrap()
                .push(format!("upload {remote}"));
            if cancel.load(Ordering::Relaxed) {
                bail!("cancelled");
            }
            if self.0.fail_upload.swap(false, Ordering::Relaxed) {
                bail!("connection lost");
            }
            for done in 0..=100 {
                progress(done, 100);
            }
            Ok(())
        }

        fn disconnect(self) {}
    }

    fn worker(state: &Arc<State>) -> Worker {
        let state = Arc::clone(state);
        Worker::spawn_with_connector(
            || {},
            move |_| {
                state.connects.fetch_add(1, Ordering::Relaxed);
                Ok(FakeConnection(Arc::clone(&state)))
            },
        )
    }

    fn send(worker: &Worker, command: Command) {
        assert!(worker.commands.send(command).is_ok());
    }

    fn next(worker: &Worker) -> Event {
        worker.events.recv_timeout(Duration::from_secs(2)).unwrap()
    }

    fn connect(worker: &Worker) {
        send(
            worker,
            Command::Connect {
                settings: ConnectionSettings {
                    host: "127.0.0.1".to_owned(),
                    port: 5000,
                    username: String::new(),
                    password: String::new(),
                },
                path: "/".to_owned(),
            },
        );
        assert!(matches!(next(worker), Event::Connected { .. }));
    }

    fn upload(worker: &Worker, id: u64) {
        send(
            worker,
            Command::Upload {
                id,
                local: PathBuf::from("game.cia"),
                remote: "/cias/game.cia".to_owned(),
            },
        );
    }

    #[test]
    fn failed_upload_invalidates_session_and_does_not_retry() {
        let state = Arc::new(State::default());
        let worker = worker(&state);
        connect(&worker);
        state.fail_upload.store(true, Ordering::Relaxed);
        upload(&worker, 7);
        assert!(matches!(next(&worker), Event::Failed { id: Some(7), .. }));
        assert!(matches!(next(&worker), Event::Disconnected));
        assert_eq!(state.uploads.load(Ordering::Relaxed), 1);
        assert_eq!(state.drops.load(Ordering::Relaxed), 1);

        send(
            &worker,
            Command::List {
                path: "/cias".to_owned(),
            },
        );
        assert!(matches!(next(&worker), Event::Listed { .. }));
        assert_eq!(state.connects.load(Ordering::Relaxed), 2);
        assert_eq!(state.uploads.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn cancellation_before_dispatch_is_not_cleared_by_worker() {
        let state = Arc::new(State::default());
        let worker = worker(&state);
        connect(&worker);
        worker.cancel.store(true, Ordering::Relaxed);
        upload(&worker, 8);
        match next(&worker) {
            Event::Failed {
                id: Some(8),
                message,
            } => assert_eq!(message, "Transfer cancelled"),
            _ => panic!("Expected the queued transfer to be cancelled"),
        }
        assert!(matches!(next(&worker), Event::Disconnected));
        assert_eq!(state.uploads.load(Ordering::Relaxed), 0);
        assert!(worker.cancel.load(Ordering::Relaxed));
    }

    #[test]
    fn fast_upload_coalesces_progress_and_keeps_final_update() {
        let state = Arc::new(State::default());
        let worker = worker(&state);
        connect(&worker);
        upload(&worker, 9);
        assert!(matches!(
            next(&worker),
            Event::Progress {
                id: 9,
                done: 0,
                total: 100
            }
        ));
        let mut updates = 1;
        let mut final_progress = false;
        loop {
            match next(&worker) {
                Event::Progress {
                    id: 9,
                    done,
                    total: 100,
                } => {
                    updates += 1;
                    final_progress = done == 100;
                }
                Event::Uploaded { id: 9 } => break,
                _ => panic!("Unexpected event during successful upload"),
            }
        }
        assert!(final_progress);
        assert!(updates < 101, "Progress should be coalesced");
    }

    #[test]
    fn failed_initial_listing_reports_failure_and_disconnects() {
        let state = Arc::new(State::default());
        state.fail_list.store(true, Ordering::Relaxed);
        let worker = worker(&state);
        send(
            &worker,
            Command::Connect {
                settings: ConnectionSettings {
                    host: "127.0.0.1".to_owned(),
                    port: 5000,
                    username: String::new(),
                    password: String::new(),
                },
                path: "/".to_owned(),
            },
        );
        match next(&worker) {
            Event::Failed { id: None, message } => assert!(message.contains("listing failed")),
            _ => panic!("Expected an initial listing failure"),
        }
        assert!(matches!(next(&worker), Event::Disconnected));
        assert_eq!(state.drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn explicit_disconnect_forgets_settings() {
        let state = Arc::new(State::default());
        let worker = worker(&state);
        connect(&worker);
        send(&worker, Command::Disconnect);
        assert!(matches!(next(&worker), Event::Disconnected));
        send(
            &worker,
            Command::List {
                path: "/".to_owned(),
            },
        );
        match next(&worker) {
            Event::Failed { id: None, message } => {
                assert!(message.contains("Connect to your 3DS first"))
            }
            _ => panic!("Disconnected commands must not reuse saved credentials"),
        }
        assert!(matches!(next(&worker), Event::Disconnected));
        assert_eq!(state.connects.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn dropping_worker_cancels_without_waiting_for_blocked_network_io() {
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let worker = Worker::spawn_with_connector(
            || {},
            move |_| {
                started_tx.send(())?;
                release_rx.recv_timeout(Duration::from_secs(5))?;
                Ok(FakeConnection(Arc::new(State::default())))
            },
        );
        send(
            &worker,
            Command::Connect {
                settings: ConnectionSettings {
                    host: "127.0.0.1".to_owned(),
                    port: 5000,
                    username: String::new(),
                    password: String::new(),
                },
                path: "/".to_owned(),
            },
        );
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let cancel = Arc::clone(&worker.cancel);
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let dropping = thread::spawn(move || {
            drop(worker);
            dropped_tx.send(()).unwrap();
        });

        // Release the network operation even if the assertion would fail, so a
        // regression cannot leave a permanently blocked test thread behind.
        let dropped = dropped_rx.recv_timeout(Duration::from_secs(2));
        let _ = release_tx.send(());
        dropping.join().unwrap();
        assert!(
            dropped.is_ok(),
            "Dropping the worker must not join its thread"
        );
        assert!(cancel.load(Ordering::Relaxed));
    }
}
