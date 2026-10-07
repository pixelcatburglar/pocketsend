//! A single, bounded-time FTP session for the 3DS FTPD homebrew app.

use std::fs::File;
use std::io::{Read, Write};
use std::net::{IpAddr, Shutdown, SocketAddr, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail, ensure};
use suppaftp::{FtpError, FtpStream, types::FileType};

const TIMEOUT: Duration = Duration::from_secs(10);
const FAT32_MAX_FILE_SIZE: u64 = u32::MAX as u64;
const BUFFER_SIZE: usize = 64 * 1024;
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct ConnectionSettings {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub name: String,
    pub size: Option<u64>,
    pub is_dir: bool,
}

pub struct Client {
    stream: Option<FtpStream>,
    use_mlsd: bool,
}

impl Client {
    pub fn connect(settings: &ConnectionSettings) -> Result<Self> {
        validate_argument(&settings.username)?;
        validate_argument(&settings.password)?;
        ensure!(
            settings.port != 0,
            "The FTP port must be between 1 and 65535"
        );
        // FTPD displays a numeric IP; avoiding DNS also bounds connection time.
        let ip: IpAddr = settings
            .host
            .trim()
            .parse()
            .context("Enter the IP address displayed by FTPD, for example 192.168.1.42")?;
        let socket = TcpStream::connect_timeout(&SocketAddr::new(ip, settings.port), TIMEOUT)
            .context("Could not reach the 3DS. Keep FTPD open and use the same Wi-Fi network")?;
        configure_socket(&socket)?;
        let mut stream = FtpStream::connect_with_stream(socket)
            .context("The device did not send an FTP greeting")?
            .passive_stream_builder(|address| {
                let socket = TcpStream::connect_timeout(&address, TIMEOUT)
                    .map_err(FtpError::ConnectionError)?;
                configure_socket(&socket).map_err(FtpError::ConnectionError)?;
                Ok(socket)
            });
        // FTPD may advertise an unusable PASV IP. Always use the control peer.
        stream.set_passive_nat_workaround(true);
        if ip.is_ipv6() {
            stream.set_mode(suppaftp::types::Mode::ExtendedPassive);
        }
        let username = if settings.username.is_empty() {
            "anonymous"
        } else {
            &settings.username
        };
        stream
            .login(username, &settings.password)
            .context("FTP login failed. Check FTPD's username and password settings")?;
        stream
            .transfer_type(FileType::Binary)
            .context("The device could not enable binary file transfers")?;
        // FTPD can read Type and Size straight from SD directory entries. Asking
        // for Modify as well would trigger slow extra timestamp reads on the 3DS.
        match stream.opts("MLST", Some("Type;Size;")) {
            Ok(()) => {}
            Err(error) if command_unsupported(&error) => {}
            Err(error) => {
                return Err(error).context("Could not configure the FTP directory listing");
            }
        }
        Ok(Self {
            stream: Some(stream),
            use_mlsd: true,
        })
    }

    pub fn list(&mut self, path: &str) -> Result<Vec<Entry>> {
        validate_remote_path(path)?;
        let result = self.list_inner(path);
        if result.is_err() {
            self.invalidate();
        }
        result.with_context(|| format!("Could not read {path}"))
    }

    fn list_inner(&mut self, path: &str) -> Result<Vec<Entry>> {
        let (lines, machine_format) = if self.use_mlsd {
            match self.stream()?.mlsd(Some(path)) {
                Ok(lines) => (lines, true),
                Err(error) if command_unsupported(&error) => {
                    self.use_mlsd = false;
                    (self.stream()?.list(Some(path))?, false)
                }
                Err(error) => return Err(error.into()),
            }
        } else {
            (self.stream()?.list(Some(path))?, false)
        };
        let mut entries = Vec::with_capacity(lines.len());
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let entry = if machine_format {
                parse_mlsd(&line)?
            } else {
                parse_list(&line)?
            };
            if let Some(entry) = entry {
                entries.push(entry);
            }
        }
        entries.sort_by(|a, b| {
            b.is_dir
                .cmp(&a.is_dir)
                .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
        });
        Ok(entries)
    }

    pub fn create_dir(&mut self, path: &str) -> Result<()> {
        let (parent, name) = split_target(path)?;
        let entries = self.list(parent)?;
        ensure_absent(&entries, name)?;
        self.stream()?
            .mkdir(path)
            .with_context(|| format!("Could not create {path}"))
    }

    pub fn upload(
        &mut self,
        local: &Path,
        remote: &str,
        cancel: &AtomicBool,
        mut progress: impl FnMut(u64, u64),
    ) -> Result<()> {
        let result = self.upload_inner(local, remote, cancel, &mut progress);
        // Never reuse a session after an uncertain transfer outcome.
        if result.is_err() {
            self.invalidate();
        }
        result
    }

    fn upload_inner(
        &mut self,
        local: &Path,
        remote: &str,
        cancel: &AtomicBool,
        progress: &mut impl FnMut(u64, u64),
    ) -> Result<()> {
        check_cancel(cancel)?;
        let (parent, name) = split_target(remote)?;
        let mut source = File::open(local).context("Could not open the local file")?;
        let metadata = source
            .metadata()
            .context("Could not read local file details")?;
        ensure!(metadata.is_file(), "Choose a file to send");
        let total = metadata.len();
        ensure!(
            total <= FAT32_MAX_FILE_SIZE,
            "This file exceeds the 3DS FAT32 limit of 4 GiB minus 1 byte"
        );
        let entries = self.list(parent)?;
        ensure_absent(&entries, name)?;
        let temporary = temporary_path(parent);
        let temporary_name = temporary.rsplit('/').next().unwrap();
        ensure_absent(&entries, temporary_name)?;
        check_cancel(cancel)?;
        let mut transfer = self.stream()?.put_with_stream(&temporary).context(
            "The 3DS could not open a temporary file. Check SD card space and permissions",
        )?;
        let copied = copy_upload(&mut source, &mut transfer, total, cancel, progress);
        if let Err(error) = copied {
            // Close the command socket before dropping TransferStream, whose destructor
            // otherwise waits for a completion reply. The partial file is never renamed.
            self.invalidate();
            let _ = transfer.get_ref().get_ref().shutdown(Shutdown::Both);
            drop(transfer);
            return Err(error.context(format!(
                "Upload stopped; an incomplete temporary file may remain at {temporary}"
            )));
        }
        transfer.finish().with_context(|| {
            format!("The 3DS did not confirm the transfer; temporary file: {temporary}")
        })?;
        check_cancel(cancel).with_context(|| format!("Temporary file retained at {temporary}"))?;

        // A successful STOR response alone is insufficient: confirm the stored byte count
        // and check again for a destination created while this transfer was running.
        let entries = self.list(parent).with_context(|| {
            format!("Could not verify the uploaded file; temporary file retained at {temporary}")
        })?;
        let uploaded = entries.iter().find(|entry| entry.name == temporary_name);
        ensure!(
            uploaded.is_some_and(|entry| !entry.is_dir && entry.size == Some(total)),
            "The uploaded file size could not be verified; temporary file retained at {temporary}"
        );
        ensure_absent(&entries, name)
            .with_context(|| format!("Temporary file retained at {temporary}"))?;
        check_cancel(cancel).with_context(|| format!("Temporary file retained at {temporary}"))?;
        self.stream()?
            .rename(temporary.as_str(), remote)
            .with_context(|| {
                format!("Could not finish the upload; temporary file retained at {temporary}")
            })?;
        Ok(())
    }

    pub fn disconnect(mut self) {
        if let Some(mut stream) = self.stream.take() {
            let _ = stream.quit();
        }
    }

    fn stream(&mut self) -> Result<&mut FtpStream> {
        self.stream
            .as_mut()
            .ok_or_else(|| anyhow!("Reconnect to the 3DS before continuing"))
    }

    fn invalidate(&mut self) {
        if let Some(stream) = self.stream.take() {
            let _ = stream.get_ref().shutdown(Shutdown::Both);
        }
    }
}

fn configure_socket(socket: &TcpStream) -> std::io::Result<()> {
    socket.set_read_timeout(Some(TIMEOUT))?;
    socket.set_write_timeout(Some(TIMEOUT))?;
    socket.set_nodelay(true)
}

fn copy_upload(
    source: &mut impl Read,
    destination: &mut impl Write,
    total: u64,
    cancel: &AtomicBool,
    progress: &mut impl FnMut(u64, u64),
) -> Result<()> {
    let mut buffer = [0_u8; BUFFER_SIZE];
    let mut sent = 0_u64;
    progress(0, total);
    loop {
        check_cancel(cancel)?;
        let count = source
            .read(&mut buffer)
            .context("Could not read the local file")?;
        if count == 0 {
            break;
        }
        ensure!(
            sent + count as u64 <= total,
            "The local file changed during transfer. Retry after it stops changing"
        );
        destination
            .write_all(&buffer[..count])
            .context("The transfer was interrupted. Check FTPD and Wi-Fi")?;
        sent += count as u64;
        progress(sent, total);
    }
    check_cancel(cancel)?;
    ensure!(
        sent == total,
        "The local file changed during transfer. Retry after it stops changing"
    );
    destination
        .flush()
        .context("Could not finish sending file data")
}

fn check_cancel(cancel: &AtomicBool) -> Result<()> {
    ensure!(!cancel.load(Ordering::Relaxed), "Transfer cancelled");
    Ok(())
}

fn validate_argument(value: &str) -> Result<()> {
    ensure!(
        !value.chars().any(|c| c == '\r' || c == '\n' || c == '\0'),
        "FTP values must not contain line breaks or NUL characters"
    );
    Ok(())
}

fn validate_remote_path(path: &str) -> Result<()> {
    validate_argument(path)?;
    ensure!(
        path.starts_with('/'),
        "Choose an absolute destination on the 3DS"
    );
    ensure!(
        !path.contains('\\'),
        "Remote paths must use forward slashes"
    );
    ensure!(
        !path.split('/').any(|part| part == ".." || part == "."),
        "Remote paths must not contain . or .. components"
    );
    Ok(())
}

fn validate_entry_name(name: &str) -> Result<()> {
    validate_argument(name)?;
    ensure!(
        !name.is_empty() && name != "." && name != ".." && !name.contains(['/', '\\']),
        "The server returned an unsafe file name"
    );
    Ok(())
}

fn split_target(path: &str) -> Result<(&str, &str)> {
    validate_remote_path(path)?;
    let (parent, name) = path
        .rsplit_once('/')
        .context("Choose a destination file name")?;
    validate_entry_name(name)?;
    ensure!(
        !name.contains(['<', '>', ':', '"', '|', '?', '*']) && !name.ends_with(['.', ' ']),
        "The file name is not supported by the 3DS SD card"
    );
    ensure!(
        name.encode_utf16().count() <= 255,
        "The file name is too long for the 3DS SD card"
    );
    Ok((if parent.is_empty() { "/" } else { parent }, name))
}

fn ensure_absent(entries: &[Entry], name: &str) -> Result<()> {
    ensure!(
        !entries
            .iter()
            .any(|entry| entry.name.to_lowercase() == name.to_lowercase()),
        "{name} already exists on the 3DS. Rename the local file or choose another folder"
    );
    Ok(())
}

fn temporary_path(parent: &str) -> String {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let sequence = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "{}/.pocket-send-{stamp:x}-{:x}-{sequence:x}.part",
        parent.trim_end_matches('/'),
        std::process::id()
    )
}

fn command_unsupported(error: &FtpError) -> bool {
    // FTPD also uses 501 for MLSD on a file; it is not an unsupported-command
    // signal and must not turn a failed directory lookup into LIST of a file.
    matches!(error, FtpError::UnexpectedResponse(response) if matches!(response.status.code(), 500 | 502 | 504))
}

fn parse_mlsd(line: &str) -> Result<Option<Entry>> {
    let (facts, name) = line
        .split_once(' ')
        .context("The server returned an invalid MLSD listing")?;
    let mut kind = None;
    let mut size = None;
    for fact in facts.split(';').filter(|part| !part.is_empty()) {
        let (key, value) = fact
            .split_once('=')
            .context("The server returned an invalid MLSD fact")?;
        if key.eq_ignore_ascii_case("type") {
            kind = Some(value.to_ascii_lowercase());
        } else if key.eq_ignore_ascii_case("size") {
            size = Some(
                value
                    .parse::<u64>()
                    .context("The server returned an invalid file size")?,
            );
        }
    }
    let kind = kind.context("The server did not identify an entry's type")?;
    if kind == "cdir" || kind == "pdir" || name == "." || name == ".." {
        return Ok(None);
    }
    ensure!(
        kind == "file" || kind == "dir",
        "The server returned an unsupported entry type: {kind}"
    );
    validate_entry_name(name)?;
    Ok(Some(Entry {
        name: name.to_owned(),
        size,
        is_dir: kind == "dir",
    }))
}

fn parse_list(line: &str) -> Result<Option<Entry>> {
    if let Some(count) = line.strip_prefix("total ")
        && count.parse::<u64>().is_ok()
    {
        return Ok(None);
    }
    let file = suppaftp::list::File::try_from(line)
        .map_err(|error| anyhow!("Could not safely parse a server directory entry: {error}"))?;
    if file.name() == "." || file.name() == ".." {
        return Ok(None);
    }
    validate_entry_name(file.name())?;
    if file.is_symlink() {
        bail!("Symbolic links are not supported on the 3DS SD card");
    }
    Ok(Some(Entry {
        name: file.name().to_owned(),
        size: Some(file.size() as u64),
        is_dir: file.is_directory(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::io::{BufRead, BufReader};
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::thread::{self, JoinHandle};
    use std::time::Instant;

    #[derive(Clone, Copy)]
    enum Behavior {
        Normal,
        ListOnly,
        DenyListing,
        FailStore,
        DestinationAppears,
    }

    #[derive(Default)]
    struct ServerState {
        files: BTreeMap<String, Vec<u8>>,
        directories: Vec<String>,
        commands: Vec<String>,
    }

    struct Server {
        settings: ConnectionSettings,
        handle: JoinHandle<ServerState>,
    }

    impl Server {
        fn start(behavior: Behavior, files: BTreeMap<String, Vec<u8>>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let handle = thread::spawn(move || {
                let mut state = ServerState {
                    files,
                    ..Default::default()
                };
                let mut control = accept_bounded(&listener);
                control
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                control
                    .set_write_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut reader = BufReader::new(control.try_clone().unwrap());
                reply(&mut control, "220 Test FTP server\r\n");
                let mut data: Option<TcpStream> = None;
                let mut rename_from: Option<String> = None;
                loop {
                    let mut line = String::new();
                    if !matches!(reader.read_line(&mut line), Ok(n) if n > 0) {
                        break;
                    }
                    let line = line.trim_end_matches(['\r', '\n']);
                    let (command, argument) = line.split_once(' ').unwrap_or((line, ""));
                    state.commands.push(command.to_owned());
                    match command {
                        "USER" => reply(&mut control, "331 Password required\r\n"),
                        "PASS" => reply(&mut control, "230 Logged in\r\n"),
                        "TYPE" => {
                            assert_eq!(argument, "I");
                            reply(&mut control, "200 Binary type enabled\r\n");
                        }
                        "OPTS" => {
                            assert_eq!(argument, "MLST Type;Size;");
                            if matches!(behavior, Behavior::ListOnly) {
                                reply(&mut control, "502 OPTS unsupported\r\n");
                            } else {
                                reply(&mut control, "200 MLST OPTS Type;Size;\r\n");
                            }
                        }
                        "PASV" => {
                            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                            let port = listener.local_addr().unwrap().port();
                            reply(
                                &mut control,
                                &format!(
                                    "227 Entering Passive Mode (127,0,0,1,{},{})\r\n",
                                    port / 256,
                                    port % 256
                                ),
                            );
                            let socket = accept_bounded(&listener);
                            socket
                                .set_read_timeout(Some(Duration::from_secs(3)))
                                .unwrap();
                            socket
                                .set_write_timeout(Some(Duration::from_secs(3)))
                                .unwrap();
                            data = Some(socket);
                        }
                        "MLSD" | "LIST" => {
                            let mut socket = data.take().unwrap();
                            if matches!(behavior, Behavior::DenyListing) {
                                reply(&mut control, "550 Permission denied\r\n");
                                continue;
                            }
                            if command == "MLSD" && matches!(behavior, Behavior::ListOnly) {
                                reply(&mut control, "502 MLSD unsupported\r\n");
                                continue;
                            }
                            reply(&mut control, "150 Directory follows\r\n");
                            let mut output = String::new();
                            if command == "MLSD" {
                                output.push_str("type=cdir; .\r\ntype=pdir; ..\r\n");
                            }
                            for (path, bytes) in &state.files {
                                let (parent, name) = path.rsplit_once('/').unwrap();
                                if parent != argument.trim_end_matches('/') {
                                    continue;
                                }
                                if command == "MLSD" {
                                    output.push_str(&format!(
                                        "type=file;size={}; {name}\r\n",
                                        bytes.len()
                                    ));
                                } else {
                                    output.push_str(&format!(
                                        "-rw-r--r-- 1 user group {} Jan 01 2026 {name}\r\n",
                                        bytes.len()
                                    ));
                                }
                            }
                            socket.write_all(output.as_bytes()).unwrap();
                            drop(socket);
                            reply(&mut control, "226 Directory sent\r\n");
                        }
                        "STOR" => {
                            reply(&mut control, "150 Opening file\r\n");
                            let mut socket = data.take().unwrap();
                            let mut bytes = Vec::new();
                            let _ = socket.read_to_end(&mut bytes);
                            state.files.insert(argument.to_owned(), bytes);
                            drop(socket);
                            if matches!(behavior, Behavior::DestinationAppears) {
                                state
                                    .files
                                    .insert("/game.cia".into(), b"another writer".to_vec());
                            }
                            if matches!(behavior, Behavior::FailStore) {
                                reply(&mut control, "552 SD card full\r\n");
                            } else {
                                reply(&mut control, "226 Transfer complete\r\n");
                            }
                        }
                        "RNFR" => {
                            rename_from = Some(argument.to_owned());
                            reply(&mut control, "350 Ready for destination\r\n");
                        }
                        "RNTO" => {
                            let bytes = state.files.remove(&rename_from.take().unwrap()).unwrap();
                            state.files.insert(argument.to_owned(), bytes);
                            reply(&mut control, "250 Rename complete\r\n");
                        }
                        "MKD" => {
                            state.directories.push(argument.to_owned());
                            reply(&mut control, "257 Directory created\r\n");
                        }
                        "QUIT" => {
                            reply(&mut control, "221 Goodbye\r\n");
                            break;
                        }
                        _ => panic!("Unexpected FTP command: {command}"),
                    }
                }
                state
            });
            Self {
                settings: ConnectionSettings {
                    host: "127.0.0.1".into(),
                    port,
                    username: String::new(),
                    password: String::new(),
                },
                handle,
            }
        }
    }

    fn reply(socket: &mut TcpStream, text: &str) {
        let _ = socket.write_all(text.as_bytes());
    }

    fn accept_bounded(listener: &TcpListener) -> TcpStream {
        listener.set_nonblocking(true).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match listener.accept() {
                Ok((stream, _)) => {
                    // Windows accepted sockets inherit the listener's nonblocking mode.
                    stream.set_nonblocking(false).unwrap();
                    return stream;
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && Instant::now() < deadline =>
                {
                    thread::sleep(Duration::from_millis(5))
                }
                Err(error) => panic!("Test FTP server accept failed: {error}"),
            }
        }
    }

    struct LocalFile(PathBuf);

    impl LocalFile {
        fn new(bytes: &[u8]) -> Self {
            let name = temporary_path("/");
            let path = std::env::temp_dir().join(name.trim_start_matches('/'));
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .unwrap();
            file.write_all(bytes).unwrap();
            Self(path)
        }
    }

    impl Drop for LocalFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    fn upload_round_trip_uses_binary_staging_and_verified_rename() {
        let bytes: Vec<u8> = (0..200_000).map(|i| (i % 251) as u8).collect();
        let local = LocalFile::new(&bytes);
        let server = Server::start(Behavior::Normal, BTreeMap::new());
        let mut client = Client::connect(&server.settings).unwrap();
        let mut updates = Vec::new();
        client
            .upload(
                &local.0,
                "/game.cia",
                &AtomicBool::new(false),
                |sent, total| updates.push((sent, total)),
            )
            .unwrap();
        assert_eq!(
            client.list("/").unwrap(),
            vec![Entry {
                name: "game.cia".into(),
                size: Some(bytes.len() as u64),
                is_dir: false
            }]
        );
        client.create_dir("/cias").unwrap();
        client.disconnect();
        let state = server.handle.join().unwrap();
        assert_eq!(state.files.get("/game.cia"), Some(&bytes));
        assert_eq!(state.files.len(), 1);
        assert_eq!(state.directories, vec!["/cias"]);
        assert_eq!(updates.first(), Some(&(0, bytes.len() as u64)));
        assert_eq!(
            updates.last(),
            Some(&(bytes.len() as u64, bytes.len() as u64))
        );
        assert!(updates.windows(2).all(|pair| pair[0].0 <= pair[1].0));
    }

    #[test]
    fn unsupported_mlsd_falls_back_to_list_once() {
        let local = LocalFile::new(b"homebrew");
        let server = Server::start(Behavior::ListOnly, BTreeMap::new());
        let mut client = Client::connect(&server.settings).unwrap();
        client
            .upload(&local.0, "/game.cia", &AtomicBool::new(false), |_, _| {})
            .unwrap();
        client.disconnect();
        let state = server.handle.join().unwrap();
        assert_eq!(state.files["/game.cia"], b"homebrew");
        assert_eq!(
            state
                .commands
                .iter()
                .filter(|command| *command == "MLSD")
                .count(),
            1
        );
        assert_eq!(
            state
                .commands
                .iter()
                .filter(|command| *command == "LIST")
                .count(),
            2
        );
    }

    #[test]
    fn existing_destination_is_never_sent_or_overwritten() {
        let local = LocalFile::new(b"new data");
        let files = BTreeMap::from([("/GAME.CIA".into(), b"original".to_vec())]);
        let server = Server::start(Behavior::Normal, files.clone());
        let mut client = Client::connect(&server.settings).unwrap();
        let error = client
            .upload(&local.0, "/game.cia", &AtomicBool::new(false), |_, _| {})
            .unwrap_err();
        assert!(error.to_string().contains("already exists"));
        client.disconnect();
        let state = server.handle.join().unwrap();
        assert_eq!(state.files, files);
        assert!(!state.commands.iter().any(|command| command == "STOR"));
    }

    #[test]
    fn permission_failure_is_not_mistaken_for_an_empty_directory() {
        let local = LocalFile::new(b"new data");
        let server = Server::start(Behavior::DenyListing, BTreeMap::new());
        let mut client = Client::connect(&server.settings).unwrap();
        assert!(
            client
                .upload(&local.0, "/game.cia", &AtomicBool::new(false), |_, _| {})
                .is_err()
        );
        client.disconnect();
        let state = server.handle.join().unwrap();
        assert!(state.files.is_empty());
        assert!(
            !state
                .commands
                .iter()
                .any(|command| command == "STOR" || command == "LIST")
        );
    }

    #[test]
    fn failed_transfer_completion_never_renames_partial_file() {
        let local = LocalFile::new(b"data");
        let server = Server::start(Behavior::FailStore, BTreeMap::new());
        let mut client = Client::connect(&server.settings).unwrap();
        assert!(
            client
                .upload(&local.0, "/game.cia", &AtomicBool::new(false), |_, _| {})
                .is_err()
        );
        client.disconnect();
        let state = server.handle.join().unwrap();
        assert_eq!(state.files.len(), 1);
        assert!(state.files.keys().all(|path| path.ends_with(".part")));
        assert!(!state.commands.iter().any(|command| command == "RNFR"));
    }

    #[test]
    fn cancelled_transfer_closes_session_and_keeps_final_name_absent() {
        let local = LocalFile::new(&vec![0x5a; BUFFER_SIZE * 3]);
        let server = Server::start(Behavior::Normal, BTreeMap::new());
        let mut client = Client::connect(&server.settings).unwrap();
        let cancel = AtomicBool::new(false);
        let error = client
            .upload(&local.0, "/game.cia", &cancel, |sent, _| {
                if sent > 0 {
                    cancel.store(true, Ordering::Relaxed);
                }
            })
            .unwrap_err();
        assert!(format!("{error:#}").contains("cancelled"));
        assert!(client.list("/").is_err());
        client.disconnect();
        let state = server.handle.join().unwrap();
        assert!(!state.files.contains_key("/game.cia"));
        assert!(
            state
                .files
                .values()
                .all(|bytes| bytes.len() < BUFFER_SIZE * 3)
        );
        assert!(!state.commands.iter().any(|command| command == "RNFR"));
    }

    #[test]
    fn destination_created_during_upload_is_preserved() {
        let local = LocalFile::new(b"our data");
        let server = Server::start(Behavior::DestinationAppears, BTreeMap::new());
        let mut client = Client::connect(&server.settings).unwrap();
        assert!(
            client
                .upload(&local.0, "/game.cia", &AtomicBool::new(false), |_, _| {})
                .is_err()
        );
        client.disconnect();
        let state = server.handle.join().unwrap();
        assert_eq!(state.files["/game.cia"], b"another writer");
        assert_eq!(state.files.len(), 2);
        assert!(!state.commands.iter().any(|command| command == "RNFR"));
    }

    #[test]
    fn unsafe_paths_and_listing_names_are_rejected() {
        for path in [
            "relative/game.cia",
            "/../game.cia",
            "/./game.cia",
            "/cia\\game.cia",
            "/game\r\nDELE /other",
        ] {
            assert!(validate_remote_path(path).is_err(), "{path:?}");
        }
        for line in [
            "type=file;size=1; ../game.cia",
            "type=file;size=1; /game.cia",
            "type=file;size=1; game\r\nDELE other",
            "type=file;size=oops; game.cia",
            "size=1; game.cia",
        ] {
            assert!(parse_mlsd(line).is_err(), "{line:?}");
        }
        assert_eq!(parse_mlsd("type=cdir; .").unwrap(), None);
        assert_eq!(
            parse_mlsd("type=file;size=42; my game.cia")
                .unwrap()
                .unwrap()
                .name,
            "my game.cia"
        );
    }

    #[test]
    fn ftpd_directory_formats_include_temporary_files() {
        // Formats emitted by mtheall/ftpd's fillDirent. Its iterator filters only
        // the literal . and .. entries; ordinary leading-dot names are included.
        let name = ".pocket-send-1234.part";
        assert_eq!(
            parse_mlsd(&format!("Type=file;Size=123; {name}"))
                .unwrap()
                .unwrap(),
            Entry {
                name: name.into(),
                size: Some(123),
                is_dir: false
            }
        );
        assert_eq!(
            parse_list(&format!("-rw-rw-rw- 0 3DS 3DS 123 Jan  1 1970 {name}"))
                .unwrap()
                .unwrap(),
            Entry {
                name: name.into(),
                size: Some(123),
                is_dir: false
            }
        );
        assert!(parse_mlsd("Type=dir;Size=0; 3ds").unwrap().unwrap().is_dir);
        assert_eq!(parse_mlsd("Type=cdir;Size=0; /cias").unwrap(), None);
        let not_directory = FtpError::UnexpectedResponse(suppaftp::types::Response::new(
            suppaftp::Status::BadArguments,
            b"501 Not a directory".to_vec(),
        ));
        assert!(!command_unsupported(&not_directory));
    }
}
