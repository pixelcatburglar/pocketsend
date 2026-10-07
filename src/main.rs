#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod app;
mod ftp;
mod worker;

fn main() -> eframe::Result {
    let options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_inner_size([1080.0, 760.0])
            .with_min_inner_size([860.0, 640.0])
            .with_drag_and_drop(true),
        renderer: eframe::Renderer::Glow,
        ..Default::default()
    };
    eframe::run_native(
        "Pocket Send",
        options,
        Box::new(|cc| Ok(Box::new(app::PocketSend::new(cc)))),
    )
}
