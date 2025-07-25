use pyo3::prelude::*;
use numpy::{IntoPyArray, PyArray1, PyArray2, PyReadonlyArray1, PyReadonlyArray2};
use ndarray::{Array1, Array2, Axis};
use std::collections::HashMap;
use rayon::prelude::*;

/// Fast histogram creation for multiple channels using Rust's performance
#[pyfunction]
fn create_histograms_batch(
    py: Python,
    qtc_data: PyReadonlyArray1<f64>,
    channel_data: PyReadonlyArray1<i32>,
    channels: Vec<i32>,
    bins: usize,
    range_min: f64,
    range_max: f64,
) -> PyResult<HashMap<i32, (Py<PyArray1<u64>>, usize)>> {
    let qtc = qtc_data.as_array();
    let channels_arr = channel_data.as_array();
    
    let bin_width = (range_max - range_min) / bins as f64;
    let mut results = HashMap::new();
    
    // Process channels in parallel
    let channel_results: Vec<_> = channels
        .par_iter()
        .filter_map(|&channel_id| {
            // Filter data for this channel
            let filtered_data: Vec<f64> = qtc
                .iter()
                .zip(channels_arr.iter())
                .filter_map(|(&qtc_val, &ch_id)| {
                    if ch_id == channel_id && qtc_val >= range_min && qtc_val < range_max {
                        Some(qtc_val)
                    } else {
                        None
                    }
                })
                .collect();
            
            if filtered_data.len() < 100 {
                return None;
            }
            
            // Create histogram
            let mut hist = vec![0u64; bins];
            for &value in &filtered_data {
                let bin_idx = ((value - range_min) / bin_width) as usize;
                if bin_idx < bins {
                    hist[bin_idx] += 1;
                }
            }
            
            Some((channel_id, hist, filtered_data.len()))
        })
        .collect();
    
    // Convert results to Python objects
    for (channel_id, hist, count) in channel_results {
        let hist_array = Array1::from_vec(hist);
        let py_hist = hist_array.into_pyarray(py).to_owned();
        results.insert(channel_id, (py_hist, count));
    }
    
    Ok(results)
}

/// Fast weighted mean calculation optimized in Rust
#[pyfunction]
fn weighted_mean_batch(
    py: Python,
    values: PyReadonlyArray2<f64>,
    weights: PyReadonlyArray2<f64>,
) -> PyResult<Py<PyArray1<f64>>> {
    let vals = values.as_array();
    let weights_arr = weights.as_array();
    
    let results: Vec<f64> = vals
        .axis_iter(Axis(0))
        .zip(weights_arr.axis_iter(Axis(0)))
        .par_bridge()
        .map(|(val_row, weight_row)| {
            let total_weight: f64 = weight_row.sum();
            if total_weight == 0.0 {
                return 0.0;
            }
            
            let weighted_sum: f64 = val_row
                .iter()
                .zip(weight_row.iter())
                .map(|(&v, &w)| v * w)
                .sum();
            
            weighted_sum / total_weight
        })
        .collect();
    
    let result_array = Array1::from_vec(results);
    Ok(result_array.into_pyarray(py).to_owned())
}

/// Fast peak finding in specified ranges
#[pyfunction]
fn find_peaks_batch(
    py: Python,
    histograms: PyReadonlyArray2<f64>,
    bin_centers: PyReadonlyArray1<f64>,
    range_min: f64,
    range_max: f64,
) -> PyResult<Py<PyArray1<f64>>> {
    let hists = histograms.as_array();
    let bins = bin_centers.as_array();
    
    let peaks: Vec<f64> = hists
        .axis_iter(Axis(0))
        .par_bridge()
        .map(|hist_row| {
            // Find indices within range
            let valid_indices: Vec<usize> = bins
                .iter()
                .enumerate()
                .filter_map(|(i, &bin_center)| {
                    if bin_center >= range_min && bin_center <= range_max {
                        Some(i)
                    } else {
                        None
                    }
                })
                .collect();
            
            if valid_indices.is_empty() {
                return 0.0;
            }
            
            // Find peak in valid range
            let mut max_val = 0.0;
            let mut max_idx = 0;
            
            for &idx in &valid_indices {
                if hist_row[idx] > max_val {
                    max_val = hist_row[idx];
                    max_idx = idx;
                }
            }
            
            bins[max_idx]
        })
        .collect();
    
    let result_array = Array1::from_vec(peaks);
    Ok(result_array.into_pyarray(py).to_owned())
}

/// Vectorized Gaussian parameter estimation
#[pyfunction]
fn estimate_gaussian_params_batch(
    py: Python,
    histograms: PyReadonlyArray2<f64>,
    bin_centers: PyReadonlyArray1<f64>,
    peak_positions: PyReadonlyArray1<f64>,
    fit_fraction_low: f64,
    fit_fraction_high: f64,
) -> PyResult<Py<PyArray2<f64>>> {
    let hists = histograms.as_array();
    let bins = bin_centers.as_array();
    let peaks = peak_positions.as_array();
    
    let params: Vec<Vec<f64>> = hists
        .axis_iter(Axis(0))
        .zip(peaks.iter())
        .par_bridge()
        .map(|(hist_row, &peak_pos)| {
            if peak_pos == 0.0 {
                return vec![0.0, 0.0, 0.0, 0.0]; // [amplitude, mean, sigma, offset]
            }
            
            let fit_min = fit_fraction_low * peak_pos;
            let fit_max = fit_fraction_high * peak_pos;
            
            // Find fitting range
            let fit_indices: Vec<usize> = bins
                .iter()
                .enumerate()
                .filter_map(|(i, &bin_center)| {
                    if bin_center >= fit_min && bin_center <= fit_max {
                        Some(i)
                    } else {
                        None
                    }
                })
                .collect();
            
            if fit_indices.len() < 10 {
                return vec![0.0, 0.0, 0.0, 0.0];
            }
            
            // Extract fitting data
            let fit_data: Vec<(f64, f64)> = fit_indices
                .iter()
                .map(|&i| (bins[i], hist_row[i]))
                .collect();
            
            // Estimate parameters
            let amplitude = fit_data.iter().map(|(_, y)| *y).fold(0.0f64, |acc, val| acc.max(val));
            let mean = peak_pos;
            
            // Estimate sigma from FWHM
            let half_max = amplitude / 2.0;
            let above_half: Vec<f64> = fit_data
                .iter()
                .filter_map(|(x, y)| if *y >= half_max { Some(*x) } else { None })
                .collect();
            
            let sigma = if above_half.len() > 1 {
                let max_val = above_half.iter().fold(0.0f64, |acc, &val| acc.max(val));
                let min_val = above_half.iter().fold(f64::INFINITY, |acc, &val| acc.min(val));
                let fwhm = max_val - min_val;
                fwhm / (2.0 * (2.0f64.ln()).sqrt())
            } else {
                (fit_max - fit_min) / 6.0
            };
            
            // Estimate offset as 10th percentile
            let mut y_values: Vec<f64> = fit_data.iter().map(|(_, y)| *y).collect();
            y_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let offset = if y_values.len() > 10 {
                y_values[y_values.len() / 10]
            } else {
                y_values[0]
            };
            
            vec![amplitude, mean, sigma, offset]
        })
        .collect();
    
    // Flatten and convert to 2D array
    let flat_params: Vec<f64> = params.into_iter().flatten().collect();
    let result_array = Array2::from_shape_vec((hists.nrows(), 4), flat_params)
        .expect("Failed to create parameter array");
    
    Ok(result_array.into_pyarray(py).to_owned())
}

/// Fast normalization calculation
#[pyfunction]
fn normalize_channels_batch(
    py: Python,
    target_means: PyReadonlyArray1<f64>,
    target_errors: PyReadonlyArray1<f64>,
    reference_means: PyReadonlyArray1<f64>,
) -> PyResult<(Py<PyArray1<f64>>, Py<PyArray1<f64>>)> {
    let targets = target_means.as_array();
    let target_errs = target_errors.as_array();
    let refs = reference_means.as_array();
    
    // Calculate reference statistics
    let ref_avg = refs.mean().unwrap_or(0.0);
    let ref_std = if refs.len() > 1 {
        let variance = refs.iter()
            .map(|x| (x - ref_avg).powi(2))
            .sum::<f64>() / (refs.len() - 1) as f64;
        variance.sqrt()
    } else {
        0.0
    };
    
    // Vectorized normalization
    let normalized: Vec<f64> = targets.iter().map(|&t| t / ref_avg).collect();
    let normalized_errors: Vec<f64> = targets.iter()
        .zip(target_errs.iter())
        .map(|(&t, &t_err)| {
            let term1 = (t_err / ref_avg).powi(2);
            let term2 = (t * ref_std / ref_avg.powi(2)).powi(2);
            (term1 + term2).sqrt()
        })
        .collect();
    
    let norm_array = Array1::from_vec(normalized);
    let err_array = Array1::from_vec(normalized_errors);
    
    Ok((
        norm_array.into_pyarray(py).to_owned(),
        err_array.into_pyarray(py).to_owned(),
    ))
}

/// Python module definition
#[pymodule]
fn data_processing_ext(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(create_histograms_batch, m)?)?;
    m.add_function(wrap_pyfunction!(weighted_mean_batch, m)?)?;
    m.add_function(wrap_pyfunction!(find_peaks_batch, m)?)?;
    m.add_function(wrap_pyfunction!(estimate_gaussian_params_batch, m)?)?;
    m.add_function(wrap_pyfunction!(normalize_channels_batch, m)?)?;
    Ok(())
}