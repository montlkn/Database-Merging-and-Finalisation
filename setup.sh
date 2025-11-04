#!/bin/bash
# Quick setup for NYC Buildings Pipeline

echo "🏗️  NYC Buildings Pipeline Setup"
echo "================================"

# Check if we're in the right directory
if [ ! -f "requirements.txt" ]; then
    echo "❌ Error: requirements.txt not found"
    echo "Run this script from the nyc-buildings-pipeline directory"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate and install
echo "Installing dependencies..."
source venv/bin/activate
pip install --upgrade pip -q
pip install -r requirements.txt -q

echo ""
echo "✅ Setup complete!"
echo ""
echo "To activate the environment:"
echo "  source venv/bin/activate"
echo ""
echo "To run the pipeline:"
echo "  python scripts/01_combine_sources.py"
echo ""
