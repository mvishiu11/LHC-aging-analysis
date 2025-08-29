# ALICE FT0 Data Processing Suite for Offline Sensor Aging Data Analysis

A comprehensive data processing and monitoring system for the ALICE FIT (Fast Interaction Trigger) detector, featuring real-time monitoring, high-performance data analysis, and automated workflow management.

> **NOTE**
> This is a WORKING repository, which means it is not very well (or at all) documented and a lot of the code may be messy.
> It was used as internal tool during develolment for ALICE FIT.
> In case of questions, please contact me at jakub.milosz.muszynski@cern.ch

## 🚀 Overview

This repository is a working repository featuring a complete pipeline for offline processing and analysis of ALICE FT0 detector data, from raw CTF files to quality control outputs. The system combines Python flexibility with Rust performance optimizations, and provides monitoring for DAQ workflows.

## 📁 Repository Structure

```bash
├── data_fetching/              # Core data acquisition
│   ├── daq_monitor.py          # Real-time system monitor GUI
│   ├── data_fetch.py           # Main CTF to digits processing pipeline
│   ├── get_*.py                # Data discovery and metadata scripts
│   └── monitor_client.py       # Dashboard communication helper
├── notebooks/                  # Analysis and optimization tools
│   ├── data_processing_ext/    # Rust performance extensions
│   ├── optimized_processing.py # Hybrid processing engine
│   ├── analysis.ipynb          # Sensor aging analysis notebook
│   ├── oxygen.ipynb            # Sensor calibration analysis for special run data
│   ├── robust_mip_fv0_r5.ipynb # Development of improved fitting strategy for FV0 ring 5
│   ├── trend_anomalies.ipynb   # EDA of trend data for vertex position anomalies
│   └── spike_detection.ipynb   # Use case notebook of anomaly detection in FIT
└── utils/                      # Batch processing utilities
    ├── make_runs_index.sh      # Run indexing for batch jobs
    └── run_o2_batch.py         # O2 workflow batch executor
```

## 🛠️ Installation

### Prerequisites

- **Python 3.11+** with scientific stack
- **Rust toolchain** (for performance extensions, optional but recommended)
- **ALICE O2 environment** for CTF processing
- **Valid Grid proxy** for ALICE data access

### Quick Setup

```bash
# Clone the repository
git clone <repository-url>
cd alice-ft0-processing

# Install Python dependencies
pip install -r requirements.txt

# Build Rust extensions (optional)
cd notebooks/data_processing_ext
maturin develop --release
cd ../..

# Set environment variables
export ALICE_BK_TOKEN="your_bookkeeping_token"
export ALIEN_HOME="/path/to/alien"
```

### Dependencies

```bash
# Core Python packages
pip install rich tqdm requests polars uproot scipy numba
pip install psutil tkinter  # For system monitoring
pip install maturin         # For Rust extensions

# Optional performance boost
pip install data_processing_ext  # After building Rust extensions
```

## 🎯 Usage

### 1. System Monitoring Dashboard

Launch the real-time system monitor to keep an eye on your processing:

```bash
python data_fetching/daq_monitor.py
```

Features a particle physics-themed interface with:
- Real-time CPU, memory, and network monitoring
- Job progress tracking with resource usage graphs
- Historical data acquisition job database
- Physics constants easter eggs (because why not?)

### 2. Data Discovery and Fetching

```bash
# Get run metadata from ALICE Bookkeeping
python data_fetching/get_dates.py -o laser_runs.json

# Map runs to AliEn storage paths
python data_fetching/get_paths.py

# Fetch and process CTF data with dashboard integration
python data_fetching/data_fetch.py \
    --list laser_paths.lst \
    --det FT0 \
    --jobs 4 \
    --cache-dir /path/to/cache
```

### 3. High-Performance Processing

For serious number crunching with hybrid Python+Rust acceleration:

```python
from notebooks.optimized_processing import process_all_runs_hybrid, create_hybrid_config

# Configure processing parameters
config = create_hybrid_config()
config.use_rust = True  # Enable 🚀 mode

# Process your data at better speed
results, normalized, metadata = process_all_runs_hybrid(
    valid_files, run_metadata, config
)
```

### 4. Batch Workflow Management

For large-scale O2 workflow running:

```bash
# Create run index
./utils/make_runs_index.sh /path/to/work/directory

# Execute batch O2 workflows with style
python utils/run_o2_batch.py runs.index \
    --cmd "o2-ctf-reader-workflow --ctf-input {lst} --onlyDet FT0 -b | o2-qc --config your-config.json -b" \
    --timeout 300 \
    --grace 30 \
    --outdir batch_logs
```

## ⚡ Performance Features

### Rust Acceleration
- **~10x faster** histogram creation for large datasets
- **Vectorized operations** for statistical calculations
- **Parallel processing** with automatic fallback to Python

### Intelligent Caching
- **Run-level caching** prevents reprocessing
- **Integrity checking** with automatic cache invalidation
- **Symlink optimization** for storage efficiency

### Memory Optimization
- **Streaming data processing** for large ROOT files
- **Polars integration** for lightning-fast DataFrame operations
- **Automatic garbage collection** during long-running jobs

## 📊 Monitoring and Logging

The system provides comprehensive monitoring through:

- **Real-time dashboard** (`daq_monitor.py`) with particle physics flair
- **Structured logging** with Rich console output
- **Performance metrics** and timing statistics
- **Progress tracking** with ETA calculations

## 🧪 Example Workflows

### Quality Control Pipeline
```bash
# 1. Discover recent runs
python data_fetching/get_dates.py --outfile recent_runs.json

# 2. Map to storage paths  
python data_fetching/get_paths.py

# 3. Process with monitoring
python data_fetching/data_fetch.py --det FT0 --jobs 8 &

# 4. Launch monitoring dashboard
python data_fetching/daq_monitor.py
```

### Performance Benchmarking
```python
from notebooks.optimized_processing import benchmark_hybrid_performance

# Compare Rust vs Python performance
benchmark_results = benchmark_hybrid_performance(
    valid_files, num_test_files=10
)
print(f"Speedup: {benchmark_results['speedup']:.1f}x")
```

## 🔧 Configuration

Key configuration files and environment variables:

- `ALICE_BK_TOKEN` - ALICE Bookkeeping authentication
- `laser_paths.lst` - Run-to-path mappings
- Cache directories for processed data
- O2 configuration files for quality control

## 📝 License

This project is part of the ALICE Collaboration software ecosystem. Please respect CERN and ALICE data policies when using this code.

---

*Built for the ALICE Collaboration. May your collisions be high-energy and your data processing be swift.*

**Pro tip**: Enable Rust extensions for maximum performance, but the Python fallbacks will keep you covered.
