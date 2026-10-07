# Pocket Send

A small native Rust app for sending local files to a Nintendo 3DS running [FTPD](https://github.com/mtheall/ftpd). No browser runtime or account required.
<img width="813" height="594" alt="image" src="https://github.com/user-attachments/assets/b0d48127-8c3e-4664-8d2f-4239b9fb423e" />
## Use it

On Windows, open `dist/pocket-send.exe`.

1. Open FTPD on your 3DS and keep it running. Connect your computer and 3DS to the same network.
2. Enter the IP address and port shown in FTPD (usually port `5000`). Leave login blank unless you configured credentials in FTPD.
3. Connect, click **Browse SD**, and select your destination. You can also enter an absolute path or create a folder. The shortcuts point to common folders; they do not create them automatically.
4. Drop files onto the window, or click **Add files**, then **Send files**.

The app copies files onto the SD card. Transferring a CIA does not install it; installation happens separately on the console. This version sends individual local files, with multiple selection supported. It does not fetch games from the internet, recursively send folders, or download files from the console.

## Build

Built and checked on Windows x64 with Rust 1.97.1. Install a current stable Rust toolchain with the C++ build tools required by Rust on Windows, then:

```powershell
cargo run --release --locked
```

The binary is `target/release/pocket-send.exe`. The native UI uses egui/eframe with the OpenGL renderer; FTP uses suppaftp. Dependencies are pinned in `Cargo.lock`. The dependencies support other desktop platforms, but this app has only been built and visually checked on Windows.

```powershell
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --locked
```

Tests cover actual transfers to local TCP FTP servers, failure and cancellation behavior, destination conflicts, FTPD listing formats, unsafe paths, and worker shutdown. Physical 3DS hardware has not been tested.
