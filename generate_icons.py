#!/usr/bin/env python3
"""
Generate multi-size .ico file from icon.png for Windows executable icon.
Requires: pip install pillow
"""

from PIL import Image
import sys

def generate_ico(input_path: str, output_path: str):
    """Convert PNG to multi-size ICO file."""
    img = Image.open(input_path)
    
    # Convert to RGBA if needed
    if img.mode != 'RGBA':
        img = img.convert('RGBA')
    
    # Generate common Windows icon sizes
    sizes = [(16, 16), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)]
    icons = []
    
    for size in sizes:
        resized = img.resize(size, Image.Resampling.LANCZOS)
        icons.append(resized)
    
    # Save as .ico with all sizes
    icons[0].save(output_path, format='ICO', sizes=[img.size for img in icons], append_images=icons[1:])
    print(f"✅ Generated {output_path} with sizes: {', '.join(f'{w}x{h}' for w, h in sizes)}")

if __name__ == '__main__':
    input_file = 'icon.png'
    output_file = 'icon.ico'
    
    try:
        generate_ico(input_file, output_file)
    except ImportError:
        print("❌ Error: Pillow not installed. Run: pip install pillow")
        sys.exit(1)
    except FileNotFoundError:
        print(f"❌ Error: {input_file} not found")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
