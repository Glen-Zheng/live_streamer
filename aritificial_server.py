#!/usr/bin/env python3
"""
Simple Flask server that serves PNG test images.

Usage:
    python3 flask_png_server.py [num_images] [color1] [color2] ...

Examples:
    python3 flask_png_server.py
    python3 flask_png_server.py 5 red green blue yellow cyan
    python3 flask_png_server.py 3 #FF0000 #00FF00 #0000FF
"""

from flask import Flask
from PIL import Image, ImageDraw
import io
import sys

app = Flask(__name__)

# Default colors
DEFAULT_COLORS = ['red', 'green', 'blue', 'yellow', 'cyan']

# Parse command line arguments
if len(sys.argv) > 1:
    try:
        num_images = int(sys.argv[1])
        if len(sys.argv) > 2:
            COLORS = sys.argv[2:2+num_images]
        else:
            # Generate default colors if not provided
            COLORS = [DEFAULT_COLORS[i % len(DEFAULT_COLORS)] for i in range(num_images)]
    except ValueError:
        print("Usage: python3 flask_png_server.py [num_images] [color1] [color2] ...")
        sys.exit(1)
else:
    COLORS = DEFAULT_COLORS

@app.route('/')
def index():
    """Serve the index page with PNG links"""
    html = f'''
<!DOCTYPE html>
<html>
<head>
    <title>PNG Crawler Test Server</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background: #f5f5f5;
        }}
        h1 {{
            color: #333;
        }}
        .png-links {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 10px;
            margin-top: 20px;
        }}
        a {{
            padding: 15px;
            background: white;
            border: 1px solid #ddd;
            text-decoration: none;
            text-align: center;
            border-radius: 5px;
            color: #333;
            transition: all 0.3s;
        }}
        a:hover {{
            background: #e8e8e8;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }}
        .info {{
            background: white;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
        }}
    </style>
</head>
<body>
    <h1>PNG Crawler Test Server</h1>
    <div class="info">
        <p>This server contains <strong>{len(COLORS)}</strong> PNG images for testing your crawler.</p>
        <p>All images are 1920x1080 pixels.</p>
    </div>
    <div class="png-links">
'''
    
    for i in range(len(COLORS)):
        html += f'        <a href="/image/{i}">PNG {i}</a>\n'
    
    html += '''
    </div>
</body>
</html>
'''
    return html

@app.route('/image/<int:image_id>')
def serve_image(image_id):
    """Generate and serve PNG image"""
    if image_id >= len(COLORS):
        return "Image not found", 404
    
    color = COLORS[image_id]
    width, height = 1920, 1080
    
    # Create image with the specified color
    img = Image.new('RGB', (width, height), color)
    draw = ImageDraw.Draw(img)
    
    # Add text with image ID and color
    text = f"Test Image #{image_id}\n({color})"
    draw.text((50, 50), text, fill='white')
    
    # Convert to PNG bytes
    img_bytes = io.BytesIO()
    img.save(img_bytes, format='PNG')
    png_data = img_bytes.getvalue()
    
    # Return PNG with correct headers
    from flask import Response
    return Response(png_data, mimetype='image/png')

if __name__ == '__main__':
    print(f"PNG Test Server starting...")
    print(f"  Images: {len(COLORS)}")
    print(f"  Colors: {', '.join(COLORS)}")
    print(f"\nServer running at http://127.0.0.1:5000")
    print(f"\nCrawl with:")
    print(f"  ./findpng2 -t 4 -m {len(COLORS)} http://127.0.0.1:5000")
    print(f"\nPress Ctrl+C to stop")
    print()
    
    app.run(host='127.0.0.1', port=5002, debug=False)