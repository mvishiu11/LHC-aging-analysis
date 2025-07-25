import numpy as np
import pandas as pd
import polars as pl
import uproot
from scipy.optimize import curve_fit
from numba import njit
from tqdm.notebook import tqdm
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import warnings
import time
import gc
warnings.filterwarnings('ignore')

# Try to import Rust extensions
try:
    import data_processing_ext
    RUST_AVAILABLE = True
    print("🚀 Rust extensions loaded - Maximum performance mode enabled!")
except ImportError:
    RUST_AVAILABLE = False
    print("⚡ Python optimization mode enabled (Rust extensions not found)")

# Configuration constants
HIST_BINS = 4100
HIST_RANGE = (0, 4100)
TARGET_CHANNELS = list(range(208))
REFERENCE_CHANNELS = [208, 210, 211]
SKIP_CHANNEL = 209
REF_PEAK_RANGE = (150, 600)
FIT_FRACTION_LOW = 0.75
FIT_FRACTION_HIGH = 1.25


@dataclass
class ProcessingConfig:
    """Configuration for processing parameters"""
    hist_bins: int = 1000
    hist_range: Tuple[float, float] = (0, 1000)
    min_entries: int = 100
    min_fit_points: int = 10
    ref_peak_range: Tuple[float, float] = (150, 600)
    fit_fraction_low: float = 0.75
    fit_fraction_high: float = 1.25
    use_rust: bool = RUST_AVAILABLE  # Enable/disable Rust acceleration


@njit
def gaussian_func_numba(x, amplitude, mean, sigma, offset):
    """Numba-compiled Gaussian function"""
    return amplitude * np.exp(-0.5 * ((x - mean) / sigma) ** 2) + offset


@njit
def weighted_mean_numba(values, weights):
    """Fast weighted mean calculation"""
    total_weight = np.sum(weights)
    if total_weight == 0:
        return 0.0, 0.0, 0.0
    
    weighted_mean = np.sum(values * weights) / total_weight
    weighted_variance = np.sum(weights * (values - weighted_mean)**2) / total_weight
    weighted_std = np.sqrt(weighted_variance)
    
    # Effective sample size
    sum_weights_sq = np.sum(weights**2)
    n_effective = total_weight**2 / sum_weights_sq if sum_weights_sq > 0 else 1.0
    mean_err = weighted_std / np.sqrt(n_effective)
    
    return weighted_mean, weighted_std, mean_err


@njit
def find_peak_in_range(hist, bin_centers, range_min, range_max):
    """Find peak position within specified range"""
    mask = (bin_centers >= range_min) & (bin_centers <= range_max)
    valid_indices = np.where(mask)[0]
    
    if len(valid_indices) == 0:
        return -1, 0.0
    
    restricted_hist = hist[valid_indices]
    peak_idx_local = np.argmax(restricted_hist)
    peak_idx_global = valid_indices[peak_idx_local]
    
    return peak_idx_global, bin_centers[peak_idx_global]


@njit
def create_fit_mask(bin_centers, peak_position, fit_fraction_low, fit_fraction_high):
    """Create mask for fitting range"""
    fit_min = fit_fraction_low * peak_position
    fit_max = fit_fraction_high * peak_position
    return (bin_centers >= fit_min) & (bin_centers <= fit_max), fit_min, fit_max


class HybridOptimizedProcessor:
    """
    Hybrid processor that uses Rust extensions when available,
    falls back to optimized Python implementations
    """
    
    def __init__(self, config: ProcessingConfig):
        self.config = config
        self.bin_edges = np.linspace(config.hist_range[0], config.hist_range[1], config.hist_bins + 1)
        self.bin_centers = (self.bin_edges[:-1] + self.bin_edges[1:]) / 2
        self.use_rust = config.use_rust and RUST_AVAILABLE
        
        # Performance tracking
        self.stats = {
            'files_processed': 0,
            'rust_histogram_calls': 0,
            'rust_normalization_calls': 0,
            'python_fallbacks': 0,
            'total_processing_time': 0.0
        }
        
        print(f"🔧 Processor initialized:")
        print(f"   Rust acceleration: {'✅' if self.use_rust else '❌'}")
        print(f"   Target channels: {len(TARGET_CHANNELS)}")
        print(f"   Reference channels: {len(REFERENCE_CHANNELS)}")
    
    def load_file_data_once(self, file_path: str) -> Optional[Tuple[np.ndarray, np.ndarray]]:
        """Load ROOT file data once and return flattened arrays"""
        try:
            with uproot.open(file_path) as file:
                tree = file["o2sim"]
                
                # Load data once with optimized settings
                qtc_ampl = tree["FT0DIGITSCH/FT0DIGITSCH.QTCAmpl"].array(library="np")
                channel_ids = tree["FT0DIGITSCH/FT0DIGITSCH.ChId"].array(library="np")
                
                # Efficient flattening
                qtc_flat = np.concatenate(qtc_ampl)
                ch_flat = np.concatenate(channel_ids)
                
                return qtc_flat, ch_flat
                
        except Exception as e:
            print(f"Error loading file {file_path}: {e}")
            return None
    
    def create_all_histograms_hybrid(self, qtc_data: np.ndarray, 
                                   channel_data: np.ndarray, 
                                   channels: List[int]) -> Dict[int, Tuple[np.ndarray, int]]:
        """Create histograms using Rust or optimized Python"""
        if self.use_rust:
            return self._create_histograms_rust(qtc_data, channel_data, channels)
        else:
            return self._create_histograms_python(qtc_data, channel_data, channels)
    
    def _create_histograms_rust(self, qtc_data: np.ndarray, 
                              channel_data: np.ndarray, 
                              channels: List[int]) -> Dict[int, Tuple[np.ndarray, int]]:
        """Create histograms using Rust extension for maximum speed"""
        try:
            # Call Rust function
            rust_results = data_processing_ext.create_histograms_batch(
                qtc_data.astype(np.float64),
                channel_data.astype(np.int32),
                channels,
                self.config.hist_bins,
                float(self.config.hist_range[0]),
                float(self.config.hist_range[1])
            )
            
            # Convert Rust results to expected format
            histograms = {}
            for channel_id, (hist_array, count) in rust_results.items():
                if count >= self.config.min_entries:
                    histograms[channel_id] = (np.array(hist_array, dtype=np.float64), count)
            
            self.stats['rust_histogram_calls'] += 1
            return histograms
            
        except Exception as e:
            print(f"⚠️  Rust histogram creation failed: {e}, falling back to Python")
            self.use_rust = False
            return self._create_histograms_python(qtc_data, channel_data, channels)
    
    def _create_histograms_python(self, qtc_data: np.ndarray, 
                                channel_data: np.ndarray, 
                                channels: List[int]) -> Dict[int, Tuple[np.ndarray, int]]:
        """Optimized Python histogram creation using Polars"""
        histograms = {}
        
        # Use Polars for ultra-fast operations
        df = pl.DataFrame({
            'qtc': qtc_data,
            'channel': channel_data
        })
        
        # Filter valid range and channels in one operation
        df_filtered = df.filter(
            (pl.col('qtc') >= self.config.hist_range[0]) & 
            (pl.col('qtc') < self.config.hist_range[1]) &
            pl.col('channel').is_in(channels)
        )
        
        # Group by channel for efficient processing
        for channel in channels:
            if channel == SKIP_CHANNEL:
                continue
            
            try:
                channel_data_filtered = df_filtered.filter(pl.col('channel') == channel)['qtc'].to_numpy()
                
                if len(channel_data_filtered) >= self.config.min_entries:
                    hist, _ = np.histogram(channel_data_filtered, bins=self.bin_edges)
                    histograms[channel] = (hist.astype(np.float64), len(channel_data_filtered))
                    
            except Exception:
                continue
        
        self.stats['python_fallbacks'] += 1
        return histograms
    
    def fit_gaussian_hybrid(self, hist: np.ndarray, channel_id: int, 
                           is_reference: bool = False) -> Optional[Dict]:
        """Optimized fitting function with Rust acceleration for batch processing"""
        if len(hist) == 0:
            return None
        
        if is_reference:
            # Reference channel: restricted peak finding
            peak_idx, peak_position = find_peak_in_range(
                hist, self.bin_centers, 
                self.config.ref_peak_range[0], 
                self.config.ref_peak_range[1]
            )
            
            if peak_idx == -1:
                return None
                
            # Gaussian fitting for reference channels
            return self._perform_gaussian_fit(hist, peak_position)
        else:
            # Target channel: weighted mean approach
            peak_idx = np.argmax(hist)
            peak_position = self.bin_centers[peak_idx]
            return self._calculate_weighted_mean(hist, peak_position)
    
    def _perform_gaussian_fit(self, hist: np.ndarray, peak_position: float) -> Optional[Dict]:
        """Optimized Gaussian fitting"""
        fit_mask, fit_min, fit_max = create_fit_mask(
            self.bin_centers, peak_position,
            self.config.fit_fraction_low, 
            self.config.fit_fraction_high
        )
        
        if np.sum(fit_mask) < self.config.min_fit_points:
            return None
        
        x_fit = self.bin_centers[fit_mask]
        y_fit = hist[fit_mask]
        
        # Better initial parameter estimation
        amplitude_guess = np.max(y_fit)
        mean_guess = peak_position
        
        # Estimate sigma from FWHM
        half_max = amplitude_guess / 2
        indices_above_half = np.where(y_fit >= half_max)[0]
        if len(indices_above_half) > 1:
            fwhm = x_fit[indices_above_half[-1]] - x_fit[indices_above_half[0]]
            sigma_guess = fwhm / (2 * np.sqrt(2 * np.log(2)))
        else:
            sigma_guess = (fit_max - fit_min) / 6
        
        offset_guess = np.percentile(y_fit, 10)
        
        try:
            popt, pcov = curve_fit(
                lambda x, a, m, s, o: a * np.exp(-0.5 * ((x - m) / s) ** 2) + o,
                x_fit, y_fit,
                p0=[amplitude_guess, mean_guess, sigma_guess, offset_guess],
                bounds=([0, fit_min, 0, 0], [np.inf, fit_max, fit_max-fit_min, np.inf]),
                maxfev=1000
            )
            
            # Calculate R-squared
            y_pred = popt[0] * np.exp(-0.5 * ((x_fit - popt[1]) / popt[2]) ** 2) + popt[3]
            ss_res = np.sum((y_fit - y_pred) ** 2)
            ss_tot = np.sum((y_fit - np.mean(y_fit)) ** 2)
            r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
            
            amplitude, mean, sigma, offset = popt
            errors = np.sqrt(np.diag(pcov))
            
            return {
                'amplitude': amplitude,
                'mean': mean,
                'sigma': sigma,
                'offset': offset,
                'amplitude_err': errors[0],
                'mean_err': errors[1],
                'sigma_err': errors[2],
                'offset_err': errors[3],
                'r_squared': r_squared,
                'peak_position': peak_position,
                'fit_range': (fit_min, fit_max),
                'method': 'gaussian_fit'
            }
            
        except Exception:
            return None
    
    def _calculate_weighted_mean(self, hist: np.ndarray, peak_position: float) -> Optional[Dict]:
        """Calculate weighted mean using numba-optimized functions"""
        fit_mask, fit_min, fit_max = create_fit_mask(
            self.bin_centers, peak_position,
            self.config.fit_fraction_low, 
            self.config.fit_fraction_high
        )
        
        if np.sum(fit_mask) < self.config.min_fit_points:
            return None
        
        x_range = self.bin_centers[fit_mask]
        y_range = hist[fit_mask]
        
        # Use numba-optimized weighted mean
        weighted_mean, weighted_std, mean_err = weighted_mean_numba(x_range, y_range)
        
        # Calculate pseudo R-squared
        total_weight = np.sum(y_range)
        y_pred_mean = np.full_like(y_range, np.sum(y_range * np.exp(-0.5 * ((x_range - weighted_mean) / weighted_std)**2)) / len(y_range))
        ss_res = np.sum((y_range - y_pred_mean)**2)
        ss_tot = np.sum((y_range - np.mean(y_range))**2)
        pseudo_r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
        
        # Effective sample size
        sum_weights_sq = np.sum(y_range**2)
        n_effective = total_weight**2 / sum_weights_sq if sum_weights_sq > 0 else 1
        
        return {
            'amplitude': np.max(y_range),
            'mean': weighted_mean,
            'sigma': weighted_std,
            'offset': 0,
            'amplitude_err': np.sqrt(np.max(y_range)),
            'mean_err': mean_err,
            'sigma_err': weighted_std / np.sqrt(2 * n_effective),
            'offset_err': 0,
            'r_squared': pseudo_r_squared,
            'peak_position': peak_position,
            'fit_range': (fit_min, fit_max),
            'method': 'weighted_mean',
            'n_effective': n_effective
        }
    
    def process_single_run_hybrid(self, file_path: str, run_number: int) -> Dict:
        """Process single run with hybrid Python+Rust optimizations"""
        start_time = time.time()
        
        # Load data once (major optimization)
        data = self.load_file_data_once(file_path)
        if data is None:
            return {}
        
        qtc_data, channel_data = data
        all_channels = TARGET_CHANNELS + REFERENCE_CHANNELS
        
        # Create all histograms in one pass (Rust or Python)
        histograms = self.create_all_histograms_hybrid(qtc_data, channel_data, all_channels)
        
        # Process fits for all channels
        results = {}
        for channel_id, (hist, entries) in histograms.items():
            is_reference = channel_id in REFERENCE_CHANNELS
            
            fit_result = self.fit_gaussian_hybrid(hist, channel_id, is_reference)
            
            if fit_result is not None:
                fit_result['entries'] = entries
                results[channel_id] = fit_result
        
        # Update performance tracking
        processing_time = time.time() - start_time
        self.stats['files_processed'] += 1
        self.stats['total_processing_time'] += processing_time
        
        return results
    
    def get_performance_stats(self) -> Dict:
        """Get performance statistics"""
        stats = self.stats.copy()
        if stats['files_processed'] > 0:
            stats['avg_time_per_file'] = stats['total_processing_time'] / stats['files_processed']
            stats['files_per_second'] = stats['files_processed'] / stats['total_processing_time']
        return stats


def calculate_normalized_means_hybrid(run_results: Dict, reference_channels: List[int],
                                    use_rust: bool = RUST_AVAILABLE) -> Dict:
    """
    Hybrid normalization using Rust when available and beneficial
    """
    if not run_results:
        return {}
    
    # Extract reference and target data
    ref_means = np.array([run_results[ref_ch]['mean'] for ref_ch in reference_channels if ref_ch in run_results])
    if len(ref_means) == 0:
        return {}
    
    target_channels = [ch for ch in TARGET_CHANNELS if ch in run_results]
    if len(target_channels) == 0:
        return {}
    
    target_means = np.array([run_results[ch]['mean'] for ch in target_channels])
    target_errors = np.array([run_results[ch]['mean_err'] for ch in target_channels])
    
    # Use Rust for normalization if available and beneficial (>10 channels)
    if use_rust and RUST_AVAILABLE and len(target_channels) > 10:
        try:
            normalized_means, normalized_errors = data_processing_ext.normalize_channels_batch(
                target_means, target_errors, ref_means
            )
            
            ref_average = np.mean(ref_means)
            ref_std = np.std(ref_means) if len(ref_means) > 1 else 0
            
            normalized_results = {}
            for i, channel_id in enumerate(target_channels):
                normalized_results[channel_id] = {
                    'normalized_mean': normalized_means[i],
                    'normalized_err': normalized_errors[i],
                    'raw_mean': target_means[i],
                    'raw_mean_err': target_errors[i],
                    'ref_average': ref_average,
                    'ref_std': ref_std,
                    'r_squared': run_results[channel_id]['r_squared'],
                    'entries': run_results[channel_id]['entries']
                }
            
            return normalized_results
            
        except Exception as e:
            print(f"⚠️  Rust normalization failed: {e}, using Python fallback")
    
    # Python vectorized fallback (still very fast)
    ref_average = np.mean(ref_means)
    ref_std = np.std(ref_means) if len(ref_means) > 1 else 0
    
    normalized_means = target_means / ref_average
    normalized_errors = np.sqrt(
        (target_errors / ref_average) ** 2 + 
        (target_means * ref_std / ref_average ** 2) ** 2
    )
    
    normalized_results = {}
    for i, channel_id in enumerate(target_channels):
        normalized_results[channel_id] = {
            'normalized_mean': normalized_means[i],
            'normalized_err': normalized_errors[i],
            'raw_mean': target_means[i],
            'raw_mean_err': target_errors[i],
            'ref_average': ref_average,
            'ref_std': ref_std,
            'r_squared': run_results[channel_id]['r_squared'],
            'entries': run_results[channel_id]['entries']
        }
    
    return normalized_results


def process_all_runs_hybrid(valid_files: List[Tuple[str, int, Dict]], 
                          run_metadata: pd.DataFrame,
                          config: ProcessingConfig) -> Tuple[Dict, Dict, pd.DataFrame]:
    """
    Main hybrid processing function with Rust acceleration
    """
    print(f"🚀 Starting hybrid processing of {len(valid_files)} files...")
    start_total_time = time.time()
    
    processor = HybridOptimizedProcessor(config)
    
    # Use polars for faster metadata operations
    metadata_pl = pl.from_pandas(run_metadata)
    available_runs = [run_num for _, run_num, _ in valid_files]
    filtered_metadata = metadata_pl.filter(pl.col('run').is_in(available_runs))
    
    all_results = {}
    all_normalized = {}
    failed_runs = []
    
    # Group by polarity
    polarities = filtered_metadata['polarity'].unique().to_list()
    
    for polarity in polarities:
        print(f"\n📊 Processing {polarity} polarity...")
        
        polarity_runs = filtered_metadata.filter(pl.col('polarity') == polarity)['run'].to_list()
        polarity_files = [(file_path, run_num, file_info) for file_path, run_num, file_info in valid_files if run_num in polarity_runs]
        
        polarity_results = {}
        polarity_normalized = {}
        successful_runs = 0
        
        # Enhanced progress bar with performance info
        pbar = tqdm(polarity_files, desc=f"{polarity} runs", 
                   bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]')
        
        for file_path, run_number, file_info in pbar:
            try:
                # Process single run with hybrid optimizations
                run_results = processor.process_single_run_hybrid(file_path, run_number)
                
                if len(run_results) > 50:  # Minimum number of successful channels
                    # Hybrid normalization (Rust or Python)
                    normalized_results = calculate_normalized_means_hybrid(
                        run_results, REFERENCE_CHANNELS, processor.use_rust
                    )
                    
                    if len(normalized_results) > 20:  # Minimum target channels
                        polarity_results[run_number] = run_results
                        polarity_normalized[run_number] = normalized_results
                        successful_runs += 1
                        
                        # Update progress bar with performance info
                        stats = processor.get_performance_stats()
                        pbar.set_postfix({
                            'Success': f"{successful_runs}/{len(polarity_files)}",
                            'Channels': len(normalized_results),
                            'Avg/file': f"{stats.get('avg_time_per_file', 0):.2f}s",
                            'Mode': '🚀' if processor.use_rust else '⚡'
                        })
                    else:
                        failed_runs.append((run_number, f"Insufficient normalized channels: {len(normalized_results)}"))
                else:
                    failed_runs.append((run_number, f"Insufficient fit results: {len(run_results)}"))
                    
            except Exception as e:
                failed_runs.append((run_number, f"Processing error: {str(e)}"))
            
            # Memory cleanup every 50 files
            if successful_runs % 50 == 0 and successful_runs > 0:
                gc.collect()
        
        # Store results by polarity
        all_results[polarity] = polarity_results
        all_normalized[polarity] = polarity_normalized
        
        print(f"✅ {polarity} complete: {successful_runs}/{len(polarity_files)} runs successful")
    
    # Final performance report
    total_time = time.time() - start_total_time
    total_successful = sum(len(results) for results in all_results.values())
    final_stats = processor.get_performance_stats()
    
    print(f"\n🎉 HYBRID PROCESSING COMPLETE!")
    print(f"📈 Performance Summary:")
    print(f"   Total time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
    print(f"   Successful runs: {total_successful}")
    print(f"   Average time per file: {final_stats.get('avg_time_per_file', 0):.2f}s")
    print(f"   Files per second: {final_stats.get('files_per_second', 0):.2f}")
    print(f"   🚀 Rust histogram calls: {final_stats['rust_histogram_calls']}")
    print(f"   🚀 Rust normalization calls: {final_stats['rust_normalization_calls']}")
    print(f"   ⚡ Python fallbacks: {final_stats['python_fallbacks']}")
    
    if failed_runs:
        print(f"⚠️  Failed runs: {len(failed_runs)}")
        if len(failed_runs) <= 10:
            for run_num, reason in failed_runs[:10]:
                print(f"   Run {run_num}: {reason}")
    
    return all_results, all_normalized, filtered_metadata.to_pandas()


# Convenience functions for easy usage
def create_hybrid_config() -> ProcessingConfig:
    """Create default hybrid configuration"""
    return ProcessingConfig(
        hist_bins=HIST_BINS,
        hist_range=HIST_RANGE,
        min_entries=100,
        min_fit_points=10,
        ref_peak_range=REF_PEAK_RANGE,
        fit_fraction_low=FIT_FRACTION_LOW,
        fit_fraction_high=FIT_FRACTION_HIGH,
        use_rust=RUST_AVAILABLE
    )


def benchmark_hybrid_performance(valid_files, num_test_files=5):
    """
    Benchmark hybrid performance vs Python-only
    """
    if len(valid_files) < num_test_files:
        print(f"Not enough files for benchmark (need {num_test_files}, have {len(valid_files)})")
        return
    
    test_files = valid_files[:num_test_files]
    config = create_hybrid_config()
    
    print(f"🏁 Benchmarking hybrid performance with {num_test_files} files...")
    
    # Test with Rust enabled
    config.use_rust = RUST_AVAILABLE
    processor_rust = HybridOptimizedProcessor(config)
    
    start_time = time.time()
    rust_results = {}
    for file_path, run_num, _ in test_files:
        results = processor_rust.process_single_run_hybrid(file_path, run_num)
        if results:
            rust_results[run_num] = results
    rust_time = time.time() - start_time
    
    # Test with Python only
    config.use_rust = False
    processor_python = HybridOptimizedProcessor(config)
    
    start_time = time.time()
    python_results = {}
    for file_path, run_num, _ in test_files:
        results = processor_python.process_single_run_hybrid(file_path, run_num)
        if results:
            python_results[run_num] = results
    python_time = time.time() - start_time
    
    # Report results
    speedup = python_time / rust_time if rust_time > 0 else 1
    
    print(f"📊 Benchmark Results:")
    print(f"   🚀 Rust mode: {rust_time:.2f}s ({rust_time/num_test_files:.2f}s per file)")
    print(f"   ⚡ Python mode: {python_time:.2f}s ({python_time/num_test_files:.2f}s per file)")
    print(f"   🏆 Speedup: {speedup:.1f}x faster with Rust")
    print(f"   📈 Estimated time for {len(valid_files)} files:")
    print(f"      Rust: {(rust_time/num_test_files)*len(valid_files)/60:.1f} minutes")
    print(f"      Python: {(python_time/num_test_files)*len(valid_files)/60:.1f} minutes")
    
    return {
        'rust_time': rust_time,
        'python_time': python_time, 
        'speedup': speedup,
        'rust_results': len(rust_results),
        'python_results': len(python_results)
    }