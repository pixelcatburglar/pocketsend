use std::{
    path::PathBuf,
    sync::{atomic::Ordering, mpsc},
    time::{Duration, Instant},
};

use eframe::egui::{self, Color32, RichText, Stroke, Vec2};
use serde::{Deserialize, Serialize};

use crate::{
    ftp::{ConnectionSettings, Entry},
    worker::{Command, Event, Worker},
};

const BG: Color32 = Color32::from_rgb(20, 24, 29);
const PANEL: Color32 = Color32::from_rgb(27, 32, 38);
const CARD: Color32 = Color32::from_rgb(32, 38, 45);
const BORDER: Color32 = Color32::from_rgb(49, 59, 68);
const TEXT: Color32 = Color32::from_rgb(232, 237, 234);
const MUTED: Color32 = Color32::from_rgb(153, 166, 173);
const MINT: Color32 = Color32::from_rgb(140, 226, 190);
const ERROR: Color32 = Color32::from_rgb(255, 162, 145);

#[derive(Serialize, Deserialize)]
#[serde(default)]
struct Preferences {
    host: String,
    port: String,
    username: String,
}

impl Default for Preferences {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: "5000".into(),
            username: String::new(),
        }
    }
}

#[derive(PartialEq)]
enum FileState {
    Queued,
    Sending,
    Sent,
    Failed(String),
    Cancelled,
}

struct QueuedFile {
    id: u64,
    local: PathBuf,
    name: String,
    size: u64,
    done: u64,
    state: FileState,
    remote: Option<String>,
    started: Option<Instant>,
}

pub struct PocketSend {
    preferences: Preferences,
    password: String,
    worker: Worker,
    connected: bool,
    busy: Option<&'static str>,
    folder: String,
    folder_input: String,
    entries: Vec<Entry>,
    show_browser: bool,
    new_folder: Option<String>,
    files: Vec<QueuedFile>,
    next_id: u64,
    running: bool,
    active: Option<u64>,
    notice: String,
    notice_is_error: bool,
    picker_tx: mpsc::Sender<Option<Vec<PathBuf>>>,
    picker_rx: mpsc::Receiver<Option<Vec<PathBuf>>>,
    picking: bool,
    confirm_close: bool,
    allow_close: bool,
    worker_unavailable: bool,
}

impl PocketSend {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        configure_style(&cc.egui_ctx);
        let preferences = cc
            .storage
            .and_then(|storage| eframe::get_value(storage, "connection"))
            .unwrap_or_default();
        let ctx = cc.egui_ctx.clone();
        let app = Self::with_worker(preferences, Worker::spawn(move || ctx.request_repaint()));
        #[cfg(feature = "screenshot")]
        if std::env::var("POCKET_SEND_PREVIEW").is_ok() {
            return app.preview();
        }
        app
    }

    fn with_worker(preferences: Preferences, worker: Worker) -> Self {
        let (picker_tx, picker_rx) = mpsc::channel();
        Self {
            preferences,
            password: String::new(),
            worker,
            connected: false,
            busy: None,
            folder: "/".into(),
            folder_input: "/".into(),
            entries: Vec::new(),
            show_browser: false,
            new_folder: None,
            files: Vec::new(),
            next_id: 1,
            running: false,
            active: None,
            notice: "Open FTPD on your 3DS to get started.".into(),
            notice_is_error: false,
            picker_tx,
            picker_rx,
            picking: false,
            confirm_close: false,
            allow_close: false,
            worker_unavailable: false,
        }
    }

    fn idle(&self) -> bool {
        self.busy.is_none() && self.active.is_none() && !self.running && !self.worker_unavailable
    }

    fn notify(&mut self, text: impl Into<String>, is_error: bool) {
        self.notice = text.into();
        self.notice_is_error = is_error;
    }

    fn send(&mut self, command: Command) -> bool {
        if self.worker.commands.send(command).is_err() {
            self.worker_failed();
            return false;
        }
        true
    }

    fn worker_failed(&mut self) {
        self.busy = None;
        self.running = false;
        self.connected = false;
        self.worker_unavailable = true;
        if let Some(id) = self.active.take()
            && let Some(file) = self.files.iter_mut().find(|file| file.id == id)
        {
            file.state = FileState::Failed(
                "The transfer worker stopped. Check the SD card before retrying.".into(),
            );
        }
        self.notify(
            "The transfer worker stopped. Please restart Pocket Send.",
            true,
        );
    }

    fn connect(&mut self) {
        let host = self.preferences.host.trim().to_owned();
        let port = match self.preferences.port.trim().parse::<u16>() {
            Ok(port) if port != 0 => port,
            _ => {
                return self.notify(
                    "Enter a port between 1 and 65535. FTPD normally uses 5000.",
                    true,
                );
            }
        };
        if host.is_empty() {
            return self.notify("Enter the IP address shown on your 3DS in FTPD.", true);
        }
        if host.contains("://") || host.contains('/') {
            return self.notify(
                "Enter just the IP address; use the separate port field.",
                true,
            );
        }
        self.busy = Some("Connecting");
        self.notify("Connecting to your 3DS…", false);
        self.send(Command::Connect {
            settings: ConnectionSettings {
                host,
                port,
                username: self.preferences.username.clone(),
                password: self.password.clone(),
            },
            path: self.folder.clone(),
        });
    }

    fn browse(&mut self, path: String) {
        if !self.connected || !self.idle() {
            return;
        }
        match normalize_folder(&path) {
            Ok(path) => {
                self.busy = Some("Reading SD card");
                self.send(Command::List { path });
            }
            Err(message) => self.notify(message, true),
        }
    }

    fn choose_files(&mut self, ctx: &egui::Context) {
        if self.picking {
            return;
        }
        self.picking = true;
        let tx = self.picker_tx.clone();
        let ctx = ctx.clone();
        std::thread::spawn(move || {
            let selection = rfd::FileDialog::new()
                .set_title("Choose files to send to your 3DS")
                .pick_files();
            let _ = tx.send(selection);
            ctx.request_repaint();
        });
    }

    fn add_files(&mut self, paths: Vec<PathBuf>) {
        let mut added = 0;
        let mut errors = Vec::new();
        for local in paths {
            if self
                .files
                .iter()
                .any(|file| file.local == local && file.state != FileState::Sent)
            {
                continue;
            }
            let Some(name) = local
                .file_name()
                .and_then(|name| name.to_str())
                .map(str::to_owned)
            else {
                errors.push("A file name could not be read as Unicode.".to_owned());
                continue;
            };
            if !valid_component(&name) {
                errors.push(format!("{name}: this name cannot be used on the SD card."));
                continue;
            }
            let metadata = match local.metadata() {
                Ok(metadata) if metadata.is_file() => metadata,
                Ok(_) => {
                    errors.push(format!(
                        "{name}: select files inside the folder to send them."
                    ));
                    continue;
                }
                Err(error) => {
                    errors.push(format!("{name}: {error}"));
                    continue;
                }
            };
            if metadata.len() > u32::MAX as u64 {
                errors.push(format!(
                    "{name}: exceeds the SD card's 4 GiB per-file limit."
                ));
                continue;
            }
            self.files.push(QueuedFile {
                id: self.next_id,
                local,
                name,
                size: metadata.len(),
                done: 0,
                state: FileState::Queued,
                remote: None,
                started: None,
            });
            self.next_id += 1;
            added += 1;
        }
        if !errors.is_empty() {
            self.notify(errors.join("\n"), true);
        } else if added > 0 {
            self.notify(
                format!(
                    "Added {added} {} to the queue.",
                    if added == 1 { "file" } else { "files" }
                ),
                false,
            );
        }
    }

    fn process_events(&mut self) {
        while let Ok(selection) = self.picker_rx.try_recv() {
            self.picking = false;
            if let Some(paths) = selection {
                self.add_files(paths);
            }
        }
        loop {
            let event = match self.worker.events.try_recv() {
                Ok(event) => event,
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    if !self.worker_unavailable {
                        self.worker_failed();
                    }
                    break;
                }
            };
            self.handle_event(event);
        }
        if self.running && self.active.is_none() && self.connected && self.busy.is_none() {
            if let Some(file) = self
                .files
                .iter_mut()
                .find(|file| file.state == FileState::Queued)
            {
                let remote = join_remote(&self.folder, &file.name);
                file.remote = Some(remote.clone());
                file.state = FileState::Sending;
                file.started = Some(Instant::now());
                file.done = 0;
                let command = Command::Upload {
                    id: file.id,
                    local: file.local.clone(),
                    remote,
                };
                self.active = Some(file.id);
                self.worker.cancel.store(false, Ordering::Relaxed);
                self.send(command);
            } else {
                self.running = false;
                self.notify("All queued files sent. You're ready to go.", false);
                // Refreshing is explicit so completion remains visible even if FTPD closes.
            }
        }
    }

    fn handle_event(&mut self, event: Event) {
        match event {
            Event::Connected { path, entries } | Event::Listed { path, entries } => {
                self.connected = true;
                self.busy = None;
                self.folder = path.clone();
                self.folder_input = path;
                self.entries = entries;
                self.notify(
                    "Connected. Choose a destination and send your files.",
                    false,
                );
            }
            Event::DirectoryCreated { path } => {
                self.connected = true;
                self.busy = None;
                self.new_folder = None;
                self.browse(path);
            }
            Event::Disconnected => {
                self.connected = false;
                self.busy = None;
                self.running = false;
                self.entries.clear();
            }
            Event::Progress { id, done, total } => {
                self.connected = true;
                if let Some(file) = self.files.iter_mut().find(|file| file.id == id) {
                    file.done = done;
                    file.size = total;
                }
            }
            Event::Uploaded { id } => {
                self.connected = true;
                if let Some(file) = self.files.iter_mut().find(|file| file.id == id) {
                    file.state = FileState::Sent;
                    file.done = file.size;
                }
                self.active = None;
                if self.worker.cancel.swap(false, Ordering::Relaxed) {
                    self.running = false;
                    self.notify(
                        "The file finished sending. Remaining files are paused.",
                        false,
                    );
                }
            }
            Event::Failed { id, message } => {
                self.connected = false;
                self.busy = None;
                self.running = false;
                let cancelled = id.is_some() && self.worker.cancel.load(Ordering::Relaxed);
                if let Some(id) = id {
                    if let Some(file) = self.files.iter_mut().find(|file| file.id == id) {
                        file.state = if cancelled {
                            FileState::Cancelled
                        } else {
                            FileState::Failed(message.clone())
                        };
                    }
                    self.active = None;
                }
                self.notify(if cancelled {
                    "Transfer stopped. Reconnect to retry; an unfinished .part file may remain on the SD card.".into()
                } else { message }, !cancelled);
            }
        }
    }

    fn header(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            handheld(ui, 42.0);
            ui.add_space(8.0);
            ui.vertical(|ui| {
                ui.label(RichText::new("Pocket Send").size(25.0).strong());
                ui.label(
                    RichText::new("A little closer to your 3DS.")
                        .color(MUTED)
                        .size(13.0),
                );
            });
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                let (label, color) = if self.active.is_some() {
                    ("SENDING", MINT)
                } else if self.busy.is_some() {
                    ("WORKING", MUTED)
                } else if self.connected {
                    ("CONNECTED", MINT)
                } else {
                    ("OFFLINE", MUTED)
                };
                egui::Frame::new()
                    .fill(CARD)
                    .corner_radius(20)
                    .inner_margin(egui::Margin::symmetric(14, 8))
                    .show(ui, |ui| {
                        ui.horizontal(|ui| {
                            let (rect, _) =
                                ui.allocate_exact_size(Vec2::splat(7.0), egui::Sense::hover());
                            ui.painter().circle_filled(rect.center(), 3.0, color);
                            ui.label(RichText::new(label).color(color).size(11.0).strong());
                        });
                    });
            });
        });
    }

    fn sidebar(&mut self, ui: &mut egui::Ui) {
        ui.label(
            RichText::new("YOUR CONSOLE")
                .color(MUTED)
                .size(11.0)
                .strong(),
        );
        ui.add_space(12.0);
        ui.label(RichText::new("Nintendo 3DS").size(20.0).strong());
        ui.label(RichText::new("FTPD over Wi-Fi").color(MUTED));
        ui.add_space(22.0);
        let editable = self.idle() && !self.connected;
        ui.add_enabled_ui(editable, |ui| {
            ui.label("IP address");
            ui.add(
                egui::TextEdit::singleline(&mut self.preferences.host)
                    .hint_text("192.168.1.42")
                    .desired_width(f32::INFINITY),
            );
            ui.add_space(10.0);
            ui.label("Port");
            ui.add(
                egui::TextEdit::singleline(&mut self.preferences.port).desired_width(f32::INFINITY),
            );
            ui.add_space(10.0);
            egui::CollapsingHeader::new("Login (optional)").show(ui, |ui| {
                ui.label("Username");
                ui.add(
                    egui::TextEdit::singleline(&mut self.preferences.username)
                        .hint_text("Anonymous")
                        .desired_width(f32::INFINITY),
                );
                ui.label("Password");
                ui.add(
                    egui::TextEdit::singleline(&mut self.password)
                        .password(true)
                        .desired_width(f32::INFINITY),
                );
                ui.label(
                    RichText::new("Password is kept only for this session.")
                        .size(11.0)
                        .color(MUTED),
                );
            });
        });
        ui.add_space(14.0);
        if self.connected {
            if ui
                .add_enabled(
                    self.idle(),
                    egui::Button::new("Disconnect").min_size(Vec2::new(ui.available_width(), 36.0)),
                )
                .clicked()
            {
                self.busy = Some("Disconnecting");
                self.send(Command::Disconnect);
                self.notify("Disconnected from your 3DS.", false);
            }
        } else {
            let label = if self.busy.is_some() {
                "Connecting…"
            } else {
                "Connect to 3DS"
            };
            if ui
                .add_enabled(
                    self.idle(),
                    primary_button(label).min_size(Vec2::new(ui.available_width(), 38.0)),
                )
                .clicked()
            {
                self.connect();
            }
        }
        ui.add_space(28.0);
        ui.separator();
        ui.add_space(16.0);
        ui.label(RichText::new("A quick start").strong());
        ui.add_space(10.0);
        for (number, text) in [
            ("01", "Open FTPD on your 3DS and leave it running."),
            ("02", "Connect this computer to the same Wi-Fi network."),
            ("03", "Enter the address from FTPD, then add your files."),
        ] {
            ui.horizontal_top(|ui| {
                ui.label(RichText::new(number).monospace().color(MINT).size(11.0));
                ui.add_space(5.0);
                ui.add(egui::Label::new(RichText::new(text).color(MUTED).size(12.0)).wrap());
            });
            ui.add_space(10.0);
        }
        ui.add_space(10.0);
        ui.label(
            RichText::new("One file at a time.\nEasy on your console.")
                .color(MUTED)
                .size(12.0),
        );
    }

    fn destination(&mut self, ui: &mut egui::Ui) {
        card().show(ui, |ui| {
            ui.set_width(ui.available_width());
            ui.horizontal(|ui| {
                ui.label(RichText::new("01").color(MINT).monospace());
                ui.label(RichText::new("Destination").size(17.0).strong());
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    let text = if self.show_browser { "Hide browser" } else { "Browse SD" };
                    if ui.add_enabled(self.connected && self.idle(), egui::Button::new(text)).clicked() {
                        self.show_browser = !self.show_browser;
                    }
                });
            });
            ui.add_space(10.0);
            ui.horizontal(|ui| {
                ui.label(RichText::new("SD").color(MINT).strong());
                ui.add_space(5.0);
                ui.label(RichText::new(&self.folder).monospace().size(15.0));
            });
            ui.label(RichText::new(if self.connected { "Queued files will be sent to this folder." } else { "Connect to browse the SD card and choose a folder." }).color(MUTED).size(12.0));
            if !self.connected && self.idle() && self.folder != "/" && ui.small_button("Reset destination to SD root").clicked() {
                self.folder = "/".into();
                self.folder_input = "/".into();
            }
            if self.show_browser && self.connected {
                ui.add_space(12.0);
                ui.separator();
                ui.add_space(8.0);
                ui.add_enabled_ui(self.idle(), |ui| {
                    ui.horizontal(|ui| {
                        if ui.add_enabled(self.folder != "/", egui::Button::new("Up")).clicked() {
                            self.browse(parent_remote(&self.folder));
                        }
                        let width = (ui.available_width() - 145.0).max(60.0);
                        let response = ui.add(egui::TextEdit::singleline(&mut self.folder_input).desired_width(width));
                        if ui.button("Go").clicked() || (response.lost_focus() && ui.input(|input| input.key_pressed(egui::Key::Enter))) {
                            self.browse(self.folder_input.clone());
                        }
                        if ui.button("Refresh").clicked() { self.browse(self.folder.clone()); }
                    });
                    ui.horizontal_wrapped(|ui| {
                        for path in ["/", "/cias", "/roms/nds", "/3ds"] {
                            if ui.small_button(path).clicked() { self.browse(path.into()); }
                        }
                        if ui.small_button("+ New folder").clicked() { self.new_folder = Some(String::new()); }
                    });
                    if let Some(mut name) = self.new_folder.take() {
                        let mut keep = true;
                        ui.horizontal(|ui| {
                            ui.add(egui::TextEdit::singleline(&mut name).hint_text("Folder name").desired_width(160.0));
                            if ui.button("Create").clicked() {
                                if valid_component(&name) {
                                    self.busy = Some("Creating folder");
                                    self.send(Command::CreateDir { path: join_remote(&self.folder, &name) });
                                    keep = false;
                                } else { self.notify("Enter a folder name without slashes or reserved characters.", true); }
                            }
                            if ui.button("Cancel").clicked() { keep = false; }
                        });
                        if keep { self.new_folder = Some(name); }
                    }
                    let mut selected = None;
                    egui::ScrollArea::vertical().id_salt("remote_entries").max_height(130.0).auto_shrink([false, true]).show(ui, |ui| {
                        if self.entries.is_empty() { ui.label(RichText::new("This folder is empty.").color(MUTED)); }
                        for entry in &self.entries {
                            ui.horizontal(|ui| {
                                if entry.is_dir {
                                    if ui.selectable_label(false, RichText::new(format!("{}/", entry.name)).color(TEXT)).clicked() {
                                        selected = Some(join_remote(&self.folder, &entry.name));
                                    }
                                } else {
                                    ui.label(RichText::new(format!("    {}", entry.name)).color(MUTED));
                                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                        if let Some(size) = entry.size { ui.label(RichText::new(format_bytes(size)).color(MUTED).size(11.0)); }
                                    });
                                }
                            });
                        }
                    });
                    if let Some(path) = selected { self.browse(path); }
                });
            }
        });
    }

    fn queue(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label(RichText::new("02").color(MINT).monospace());
            ui.label(RichText::new("Send queue").size(17.0).strong());
            let count = self.files.len();
            if count > 0 {
                ui.label(
                    RichText::new(format!(
                        "{count} {}",
                        if count == 1 { "file" } else { "files" }
                    ))
                    .color(MUTED)
                    .size(12.0),
                );
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui
                    .add_enabled(!self.picking, egui::Button::new("+ Add files"))
                    .clicked()
                {
                    self.choose_files(ui.ctx());
                }
                if self.files.iter().any(|file| file.state == FileState::Sent)
                    && ui.small_button("Clear sent").clicked()
                {
                    self.files.retain(|file| file.state != FileState::Sent);
                }
            });
        });
        ui.add_space(12.0);
        if self.files.is_empty() {
            let response = egui::Frame::new()
                .fill(PANEL)
                .stroke(Stroke::new(1.0, BORDER))
                .corner_radius(12)
                .show(ui, |ui| {
                    ui.set_width(ui.available_width());
                    ui.set_min_height(192.0);
                    ui.add_space(27.0);
                    ui.vertical_centered(|ui| {
                        upload_icon(ui);
                        ui.add_space(6.0);
                        ui.label(RichText::new("Drop your files here").size(21.0).strong());
                        ui.add_space(6.0);
                        ui.label(
                            RichText::new("or click to choose files from your computer")
                                .color(MUTED),
                        );
                        ui.add_space(12.0);
                        ui.label(
                            RichText::new("CIA  ·  3DSX  ·  NDS  ·  and any other files")
                                .color(MUTED)
                                .size(11.0),
                        );
                    });
                })
                .response
                .interact(egui::Sense::click());
            if response.clicked() {
                self.choose_files(ui.ctx());
            }
        } else {
            let mut remove = None;
            let mut retry = None;
            let max_height = (ui.available_height() - 80.0).max(100.0);
            egui::ScrollArea::vertical()
                .id_salt("send_queue")
                .max_height(max_height)
                .auto_shrink([false, true])
                .show(ui, |ui| {
                    for file in &self.files {
                        ui.push_id(file.id, |ui| {
                            egui::Frame::new()
                                .fill(PANEL)
                                .corner_radius(9)
                                .inner_margin(12)
                                .show(ui, |ui| {
                                    ui.set_width(ui.available_width());
                                    ui.horizontal(|ui| {
                                        let extension = file
                                            .local
                                            .extension()
                                            .and_then(|ext| ext.to_str())
                                            .unwrap_or("FILE")
                                            .to_uppercase();
                                        egui::Frame::new()
                                            .fill(CARD)
                                            .corner_radius(5)
                                            .inner_margin(8)
                                            .show(ui, |ui| {
                                                ui.label(
                                                    RichText::new(
                                                        extension
                                                            .chars()
                                                            .take(5)
                                                            .collect::<String>(),
                                                    )
                                                    .color(MINT)
                                                    .monospace()
                                                    .size(10.0),
                                                );
                                            });
                                        ui.add_space(6.0);
                                        let details_width =
                                            (ui.available_width() - 105.0).max(80.0);
                                        ui.allocate_ui_with_layout(
                                            Vec2::new(details_width, 42.0),
                                            egui::Layout::top_down(egui::Align::Min),
                                            |ui| {
                                                ui.set_max_width(details_width);
                                                ui.add(
                                                    egui::Label::new(
                                                        RichText::new(&file.name).strong(),
                                                    )
                                                    .truncate(),
                                                )
                                                .on_hover_text(file.local.display().to_string());
                                                let detail = match &file.state {
                                                    FileState::Queued => format!(
                                                        "{}  ·  Ready to send",
                                                        format_bytes(file.size)
                                                    ),
                                                    FileState::Sending => {
                                                        let seconds = file
                                                            .started
                                                            .map(|time| {
                                                                time.elapsed().as_secs_f64()
                                                            })
                                                            .unwrap_or(0.0);
                                                        let speed = if seconds > 0.0 {
                                                            (file.done as f64 / seconds) as u64
                                                        } else {
                                                            0
                                                        };
                                                        let remaining = file
                                                            .size
                                                            .saturating_sub(file.done)
                                                            .checked_div(speed)
                                                            .map(|seconds| {
                                                                format!(
                                                                    "  ·  {} left",
                                                                    format_duration(seconds)
                                                                )
                                                            })
                                                            .unwrap_or_default();
                                                        format!(
                                                            "{} / {}  ·  {}/s{remaining}",
                                                            format_bytes(file.done),
                                                            format_bytes(file.size),
                                                            format_bytes(speed)
                                                        )
                                                    }
                                                    FileState::Sent => format!(
                                                        "Sent to {}",
                                                        file.remote.as_deref().unwrap_or("SD card")
                                                    ),
                                                    FileState::Failed(_) => {
                                                        "Could not send · hover for details".into()
                                                    }
                                                    FileState::Cancelled => {
                                                        "Stopped · reconnect to retry".into()
                                                    }
                                                };
                                                let color =
                                                    if matches!(file.state, FileState::Failed(_)) {
                                                        ERROR
                                                    } else if file.state == FileState::Sent {
                                                        MINT
                                                    } else {
                                                        MUTED
                                                    };
                                                let response = ui.add(
                                                    egui::Label::new(
                                                        RichText::new(detail)
                                                            .color(color)
                                                            .size(11.0),
                                                    )
                                                    .truncate(),
                                                );
                                                if let FileState::Failed(error) = &file.state {
                                                    response.on_hover_text(error);
                                                }
                                            },
                                        );
                                        ui.with_layout(
                                            egui::Layout::right_to_left(egui::Align::Center),
                                            |ui| {
                                                if file.state == FileState::Sending {
                                                    ui.label(
                                                        RichText::new("Sending")
                                                            .color(MINT)
                                                            .size(11.0),
                                                    );
                                                } else {
                                                    if ui
                                                        .small_button("×")
                                                        .on_hover_text("Remove from queue")
                                                        .clicked()
                                                    {
                                                        remove = Some(file.id);
                                                    }
                                                    if matches!(
                                                        file.state,
                                                        FileState::Failed(_) | FileState::Cancelled
                                                    ) && ui.small_button("Retry").clicked()
                                                    {
                                                        retry = Some(file.id);
                                                    }
                                                    if file.state == FileState::Sent {
                                                        ui.label(
                                                            RichText::new("Done")
                                                                .color(MINT)
                                                                .size(11.0),
                                                        );
                                                    }
                                                }
                                            },
                                        );
                                    });
                                    if file.state == FileState::Sending {
                                        ui.add_space(8.0);
                                        let fraction = if file.size > 0 {
                                            file.done as f32 / file.size as f32
                                        } else {
                                            0.0
                                        };
                                        ui.add(
                                            egui::ProgressBar::new(fraction)
                                                .fill(MINT)
                                                .desired_height(5.0),
                                        );
                                    }
                                });
                            ui.add_space(7.0);
                        });
                    }
                });
            if let Some(id) = remove {
                self.files.retain(|file| file.id != id);
            }
            if let Some(id) = retry {
                if let Some(file) = self.files.iter_mut().find(|file| file.id == id) {
                    file.state = FileState::Queued;
                    file.done = 0;
                }
                self.notify(
                    "File queued again. Connect and press Send files when you're ready.",
                    false,
                );
            }
        }
        ui.add_space(14.0);
        ui.horizontal(|ui| {
            let waiting = self
                .files
                .iter()
                .filter(|file| file.state == FileState::Queued)
                .count();
            let bytes: u64 = self
                .files
                .iter()
                .filter(|file| file.state == FileState::Queued)
                .map(|file| file.size)
                .sum();
            ui.label(
                RichText::new(if waiting > 0 {
                    format!("{waiting} ready  ·  {}", format_bytes(bytes))
                } else {
                    "Drop files anywhere to add them.".into()
                })
                .color(MUTED)
                .size(12.0),
            );
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if self.active.is_some() {
                    let stopping = self.worker.cancel.load(Ordering::Relaxed);
                    if ui
                        .add_enabled(
                            !stopping,
                            egui::Button::new(if stopping {
                                "Stopping…"
                            } else {
                                "Stop transfer"
                            })
                            .min_size(Vec2::new(145.0, 38.0)),
                        )
                        .clicked()
                    {
                        self.running = false;
                        self.worker.cancel.store(true, Ordering::Relaxed);
                        self.notify("Stopping the transfer…", false);
                    }
                } else if ui
                    .add_enabled(
                        self.connected && self.idle() && waiting > 0,
                        primary_button("Send files").min_size(Vec2::new(145.0, 38.0)),
                    )
                    .clicked()
                {
                    self.running = true;
                    self.show_browser = false;
                    self.notify(format!("Sending files to {}", self.folder), false);
                    ui.ctx().request_repaint();
                }
            });
        });
    }
}

impl eframe::App for PocketSend {
    fn logic(&mut self, _ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.process_events();
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        let dropped = ctx.input(|input| {
            input
                .raw
                .dropped_files
                .iter()
                .map(|file| file.path().to_path_buf())
                .collect::<Vec<_>>()
        });
        if !dropped.is_empty() {
            self.add_files(dropped);
        }
        if ctx.input(|input| input.viewport().close_requested())
            && self.active.is_some()
            && !self.allow_close
        {
            ctx.send_viewport_cmd(egui::ViewportCommand::CancelClose);
            self.confirm_close = true;
        }
        egui::Panel::top("header")
            .resizable(false)
            .frame(
                egui::Frame::new()
                    .fill(BG)
                    .inner_margin(egui::Margin::symmetric(26, 20)),
            )
            .show(ui, |ui| self.header(ui));
        egui::Panel::bottom("footer")
            .resizable(false)
            .frame(
                egui::Frame::new()
                    .fill(BG)
                    .inner_margin(egui::Margin::symmetric(24, 12)),
            )
            .show(ui, |ui| {
                egui::ScrollArea::vertical()
                    .id_salt("notice")
                    .max_height(65.0)
                    .show(ui, |ui| {
                        ui.label(
                            RichText::new(&self.notice)
                                .size(12.0)
                                .color(if self.notice_is_error { ERROR } else { MUTED }),
                        );
                    });
            });
        egui::Panel::left("connection")
            .exact_size(252.0)
            .resizable(false)
            .frame(egui::Frame::new().fill(PANEL).inner_margin(22))
            .show(ui, |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| self.sidebar(ui));
            });
        egui::CentralPanel::default().frame(egui::Frame::new().fill(BG).inner_margin(24)).show(ui, |ui| {
            egui::ScrollArea::vertical().id_salt("main").auto_shrink([false, false]).show(ui, |ui| {
                self.destination(ui);
                ui.add_space(23.0);
                self.queue(ui);
                ui.add_space(18.0);
                ui.label(RichText::new("Files are copied to your SD card. Install CIA files separately on your 3DS.").size(11.0).color(MUTED));
            });
        });
        if ctx.input(|input| !input.raw.hovered_files.is_empty()) {
            egui::Area::new(egui::Id::new("drop_overlay"))
                .order(egui::Order::Tooltip)
                .anchor(egui::Align2::CENTER_CENTER, Vec2::ZERO)
                .show(&ctx, |ui| {
                    card().stroke(Stroke::new(2.0, MINT)).show(ui, |ui| {
                        ui.label(
                            RichText::new("Release to add files to your queue")
                                .size(22.0)
                                .color(MINT),
                        );
                    });
                });
        }
        if self.confirm_close {
            egui::Window::new("A file is still sending").collapsible(false).resizable(false).anchor(egui::Align2::CENTER_CENTER, Vec2::ZERO).show(&ctx, |ui| {
                ui.label("Quitting will stop the transfer. A partial file may remain on the SD card.");
                ui.add_space(12.0);
                ui.horizontal(|ui| {
                    if ui.add(primary_button("Keep sending")).clicked() { self.confirm_close = false; }
                    if ui.button("Stop and quit").clicked() {
                        self.worker.cancel.store(true, Ordering::Relaxed);
                        self.allow_close = true;
                        ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                    }
                });
            });
        }
        if self.active.is_some() || self.busy.is_some() {
            ctx.request_repaint_after(Duration::from_millis(100));
        }
    }

    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        #[cfg(feature = "screenshot")]
        if std::env::var("EFRAME_SCREENSHOT_TO").is_ok() {
            return;
        }
        eframe::set_value(storage, "connection", &self.preferences);
    }
}

// Sample data exists only in developer screenshot builds, never the release binary.
#[cfg(feature = "screenshot")]
impl PocketSend {
    fn preview(mut self) -> Self {
        self.connected = true;
        self.preferences.host = "192.168.1.42".into();
        self.folder = "/cias".into();
        self.folder_input = self.folder.clone();
        self.show_browser = std::env::var("POCKET_SEND_PREVIEW").as_deref() == Ok("browser");
        self.entries = vec![
            Entry {
                name: "homebrew".into(),
                size: None,
                is_dir: true,
            },
            Entry {
                name: "readme.txt".into(),
                size: Some(2400),
                is_dir: false,
            },
        ];
        self.files = [
            ("Moonlight.cia", 180_000_000, FileState::Sent),
            (
                "A rather long homebrew filename to check the transfer queue layout.cia",
                430_000_000,
                FileState::Sending,
            ),
            ("puzzle.3dsx", 8_000_000, FileState::Queued),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (name, size, state))| QueuedFile {
            id: index as u64 + 1,
            local: PathBuf::from(name),
            name: name.into(),
            size,
            done: size / 3,
            state,
            remote: Some(join_remote("/cias", name)),
            started: Some(Instant::now() - Duration::from_secs(100)),
        })
        .collect();
        self.active = if self.show_browser { None } else { Some(2) };
        self.next_id = 4;
        self.notice = "Layout preview with sample files. No transfer is running.".into();
        self
    }
}

fn configure_style(ctx: &egui::Context) {
    ctx.set_theme(egui::Theme::Dark);
    ctx.global_style_mut(|style| {
        style.visuals = egui::Visuals::dark();
        style.visuals.override_text_color = Some(TEXT);
        style.visuals.panel_fill = BG;
        style.visuals.window_fill = PANEL;
        style.visuals.extreme_bg_color = BG;
        style.visuals.faint_bg_color = CARD;
        style.visuals.selection.bg_fill = Color32::from_rgb(44, 83, 69);
        style.visuals.selection.stroke = Stroke::new(1.0, MINT);
        style.visuals.widgets.inactive.bg_fill = CARD;
        style.visuals.widgets.inactive.weak_bg_fill = CARD;
        style.visuals.widgets.inactive.bg_stroke = Stroke::new(1.0, BORDER);
        style.visuals.widgets.hovered.bg_fill = Color32::from_rgb(48, 63, 64);
        style.visuals.widgets.hovered.weak_bg_fill = Color32::from_rgb(48, 63, 64);
        style.visuals.widgets.hovered.bg_stroke = Stroke::new(1.0, MINT);
        for widget in [
            &mut style.visuals.widgets.inactive,
            &mut style.visuals.widgets.hovered,
            &mut style.visuals.widgets.active,
        ] {
            widget.corner_radius = egui::CornerRadius::same(6);
        }
        style.spacing.item_spacing = Vec2::new(8.0, 7.0);
        style.spacing.button_padding = Vec2::new(12.0, 7.0);
        style.spacing.interact_size.y = 30.0;
        style
            .text_styles
            .insert(egui::TextStyle::Body, egui::FontId::proportional(14.0));
        style
            .text_styles
            .insert(egui::TextStyle::Button, egui::FontId::proportional(13.0));
    });
}

fn card() -> egui::Frame {
    egui::Frame::new()
        .fill(CARD)
        .stroke(Stroke::new(1.0, BORDER))
        .corner_radius(12)
        .inner_margin(18)
}

fn primary_button(text: &str) -> egui::Button<'_> {
    egui::Button::new(RichText::new(text).color(BG).strong())
        .fill(MINT)
        .stroke(Stroke::NONE)
}

fn handheld(ui: &mut egui::Ui, size: f32) {
    let (rect, _) = ui.allocate_exact_size(Vec2::splat(size), egui::Sense::hover());
    let painter = ui.painter();
    let top = egui::Rect::from_min_size(
        rect.min + Vec2::new(2.0, 1.0),
        Vec2::new(size - 4.0, size * 0.44),
    );
    let bottom = egui::Rect::from_min_size(
        rect.min + Vec2::new(0.0, size * 0.51),
        Vec2::new(size, size * 0.44),
    );
    painter.rect_stroke(top, 4, Stroke::new(1.6, MINT), egui::StrokeKind::Inside);
    painter.rect_filled(top.shrink(5.0), 2, Color32::from_rgb(48, 85, 73));
    painter.rect_stroke(bottom, 4, Stroke::new(1.6, MINT), egui::StrokeKind::Inside);
    painter.rect_filled(
        bottom.shrink2(Vec2::new(11.0, 4.0)),
        2,
        Color32::from_rgb(48, 85, 73),
    );
    painter.line_segment(
        [
            bottom.left_center() + Vec2::new(4.0, 0.0),
            bottom.left_center() + Vec2::new(9.0, 0.0),
        ],
        Stroke::new(1.2, MINT),
    );
    painter.line_segment(
        [
            bottom.left_center() + Vec2::new(6.5, -2.5),
            bottom.left_center() + Vec2::new(6.5, 2.5),
        ],
        Stroke::new(1.2, MINT),
    );
    painter.circle_filled(bottom.right_center() + Vec2::new(-6.0, -1.0), 1.5, MINT);
}

fn upload_icon(ui: &mut egui::Ui) {
    let (rect, _) = ui.allocate_exact_size(Vec2::splat(34.0), egui::Sense::hover());
    let center = rect.center();
    let painter = ui.painter();
    let stroke = Stroke::new(2.0, MINT);
    painter.line_segment(
        [center + Vec2::new(0.0, 6.0), center + Vec2::new(0.0, -12.0)],
        stroke,
    );
    painter.line(
        vec![
            center + Vec2::new(-6.0, -5.0),
            center + Vec2::new(0.0, -12.0),
            center + Vec2::new(6.0, -5.0),
        ],
        stroke,
    );
    painter.line(
        vec![
            center + Vec2::new(-12.0, 4.0),
            center + Vec2::new(-12.0, 12.0),
            center + Vec2::new(12.0, 12.0),
            center + Vec2::new(12.0, 4.0),
        ],
        stroke,
    );
}

fn valid_component(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.ends_with([' ', '.'])
        && !name
            .chars()
            .any(|c| c.is_control() || "/\\:*?\"<>|".contains(c))
}

fn normalize_folder(path: &str) -> Result<String, &'static str> {
    if !path.starts_with('/') {
        return Err("SD card paths start with /.");
    }
    let components: Vec<_> = path.split('/').filter(|part| !part.is_empty()).collect();
    if components.iter().any(|part| !valid_component(part)) {
        return Err("Use a folder path without parent traversal or reserved characters.");
    }
    Ok(format!("/{}", components.join("/")))
}

fn join_remote(folder: &str, name: &str) -> String {
    format!("{}/{}", folder.trim_end_matches('/'), name)
}

fn parent_remote(folder: &str) -> String {
    match folder.trim_end_matches('/').rsplit_once('/') {
        Some(("", _)) | None => "/".into(),
        Some((parent, _)) => parent.into(),
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let units = ["KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    for unit in units {
        value /= 1024.0;
        if value < 1024.0 || unit == "TiB" {
            return format!("{value:.1} {unit}");
        }
    }
    unreachable!()
}

fn format_duration(seconds: u64) -> String {
    if seconds >= 3600 {
        format!("{}h {}m", seconds / 3600, seconds % 3600 / 60)
    } else if seconds >= 60 {
        format!("{}m {}s", seconds / 60, seconds % 60)
    } else {
        format!("{seconds}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sending_app() -> PocketSend {
        let mut app = PocketSend::with_worker(Preferences::default(), Worker::spawn(|| {}));
        app.connected = true;
        app.folder = "/cias".into();
        app.active = Some(1);
        app.running = true;
        app.files.push(QueuedFile {
            id: 1,
            local: PathBuf::from("game.cia"),
            name: "game.cia".into(),
            size: 100,
            done: 50,
            state: FileState::Sending,
            remote: Some("/cias/game.cia".into()),
            started: Some(Instant::now()),
        });
        app
    }

    #[test]
    fn failed_upload_pauses_immediately_and_preserves_destination() {
        let mut app = sending_app();
        app.handle_event(Event::Failed {
            id: Some(1),
            message: "Connection lost".into(),
        });
        assert!(!app.connected);
        assert!(!app.running);
        assert!(app.active.is_none());
        assert_eq!(app.folder, "/cias");
        assert!(matches!(app.files[0].state, FileState::Failed(_)));
    }

    #[test]
    fn stop_racing_completion_does_not_leave_stopping_status() {
        let mut app = sending_app();
        app.worker.cancel.store(true, Ordering::Relaxed);
        app.notice = "Stopping the transfer…".into();
        app.handle_event(Event::Uploaded { id: 1 });
        assert!(app.files[0].state == FileState::Sent);
        assert!(!app.running);
        assert!(app.active.is_none());
        assert!(!app.worker.cancel.load(Ordering::Relaxed));
        assert!(app.notice.contains("finished"));
    }

    #[test]
    fn worker_failure_releases_active_transfer_and_disables_dispatch() {
        let mut app = sending_app();
        app.worker_failed();
        assert!(app.active.is_none());
        assert!(!app.idle());
        assert!(!app.running);
        assert!(!app.connected);
        assert!(matches!(app.files[0].state, FileState::Failed(_)));
    }

    #[test]
    fn remote_paths_stay_inside_the_selected_folder() {
        assert_eq!(normalize_folder("//roms//nds/"), Ok("/roms/nds".into()));
        assert_eq!(join_remote("/", "a game.nds"), "/a game.nds");
        assert_eq!(parent_remote("/roms/nds"), "/roms");
        assert_eq!(parent_remote("/roms"), "/");
        for path in ["cias", "/../cias", "/a\r\nDELE game", "/a\\b"] {
            assert!(normalize_folder(path).is_err());
        }
        for name in ["", ".", "..", "a/b", "a\\b", "bad?", "name."] {
            assert!(!valid_component(name));
        }
        assert!(valid_component("Pokémon homebrew.cia"));
    }
}
