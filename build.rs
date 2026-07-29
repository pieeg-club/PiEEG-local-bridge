// Build script to embed Windows icon into the executable

fn main() {
    // Embed Windows resource file (icon) on Windows builds
    #[cfg(windows)]
    {
        // Check if icon.ico exists, if not try to generate it
        if !std::path::Path::new("icon.ico").exists() {
            println!("cargo:warning=icon.ico not found, run: python generate_icons.py");
        } else {
            // Use winres to embed the icon
            let mut res = winres::WindowsResource::new();
            res.set_icon("icon.ico");
            if let Err(e) = res.compile() {
                println!("cargo:warning=Failed to embed icon: {}", e);
            }
        }
    }
    
    // Re-run if icon files change
    println!("cargo:rerun-if-changed=icon.png");
    println!("cargo:rerun-if-changed=icon.ico");
}
