# ALICE FIT QC Guide

We use following commands for testing:

o2-fv0-digit-reader-workflow | o2-qc --config json://$HOME/alice/QualityControl/Modules/FIT/FV0/etc/fv0-amplitude-post-processing-quick.json -b

o2-ft0-digits-reader-workflow | o2-qc --config json://$HOME/alice/QualityControl/Modules/FIT/FT0/etc/ft0-test.json -b

o2-sim-digitizer-workflow -b --onlyDet FT0 --bcPatternFile ./bcPattern_550483.roo2-fv0-digit-reader-workflow | o2-qc --config json://$HOME/alice/QualityControl/Modules/FIT/FV0/etc/fv0-amplitude-post-processing.json -bnteractionRate 700000 --configKeyValues "FT0DigParam.mMip_in_V=7.;FT0DigParam.mMV_2_Nchannels=2.;FT0DigParam.mMV_2_NchannelsInverse=0.5"

o2-sim -e TGeant4 -m FV0 FT0 -j 10 -g pythia8 --seed 1 -n 5000 --field 5 --configKeyValues "Diamond.width[2]=6.;GeneratorPythia8.config=~/alice/O2/Generators/share/egconfig/pythia8_inel.cfg"

o2-sim-digitizer-workflow -b --onlyDet FV0 --bcPatternFile ./bcPattern_550483.root --interactionRate 500000 --configKeyValues “FV0DigParam.adcChannelsPerMip=16”

o2-ctf-reader-workflow --ctf-input /alice/data/2024/LHC24aa_FT0/546923/calib/1710/o2_ctf_run00546923_orbit0000000288_tf0000000001_epn348.root --copy-cmd no-copy --ctf-dict ccdb --onlyDet FT0 --severity=error -b | o2-ft0-reco-workflow --disable-mc --disable-root-input --disable-root-output -b | o2-qc --config json://$HOME/alice/QualityControl/Modules/FIT/FT0/etc/ft0-test.json -b

~/alice/sw/BUILD/QualityControl-latest-feat-extra-amplitude-graphs/QualityControl                                                                                              16:36:24
❯ source ~/alice/sw/ubuntu2404_x86-64/QualityControl/latest/etc/profile.d/init.sh

## ROOT stuff:

```
root [4] TFile f("run_564472_ft0digits.root");
root [5] TTree *t = nullptr;
root [6] f.GetObject("o2sim", t);
root [7] t->Print();
t->Draw("FT0DIGITSCH.QTCAmpl >> h_ch5(4100, 0, 4100)", "FT0DIGITSCH.getChannelID()>=0&&FT0DIGITSCH.getChannelID()<=31")
```

## TODO List

* [X] LASER run data for aging

  * [X] Plot per PM
  * [X] Plot per channel
  * [X] Write to ROOT file per channels
  * [X] Apply reference channel corrections
  * [X] Plot first to last by PM on fancy visualization
  * [X] Plot by polarity
* [X] HV config talk
* [X] Special run data for aging

  * [X] Plot for rings
  * [X] Plot for all channels
  * [X] Plot by polarity
  * [X] Plot by beam type
* [ ] Updates after talking to Yury

  * [X] Use weighted mean for LASER target channel
  * [X] Changes to ADC/MIP trending plots
    * [X] Switch ADC/MIP trending to expected gain level (14 for FT0)
    * [X] Change plot title
    * [X] Change plot legend
    * [X] Reduce stat box
  * [X] Implement HV period correction for LASER data
    * [X] Normalize the data period-wise based on HV config changes
    * [X] Cut out data before the first HV change
    * [X] Plot based on regional normalization
  * [X] Gather the rest of LASER data
  * [ ] Develop online trending of ADC/MIP as a PostProcTask
  * [ ] Figure out the user-facing components for ADC/MIP trending
  * [ ] Figure out the user-facing components for ADC/MIP ageing trending
* [ ] ADC/MIP for FV0

  * [ ] Meet with Varlan to determine user needs
  * [ ] Verify the thresholds for checkers (+- 1 MIP or 1/num of channels?)
* [X] Optimize the workflow

  * [X] Optimized data taking
  * [X] Optimized data processing

## Aging over time / charge/area expressed via MIP/ADC change

As the sensors age (over continous usage and thus radiation), the ADCch/MIP ratio changes. By tracking this change we can apply predicitive maintenance to the detector as well as define characteristics of the sensor. Data related to this will not be gathered during LHC operation, because of HV (High Voltage) correction, but for the last few years it has been gathered via laser discharge on the sensors. Due to the stochastical nature of laser light (distribution of light rays in the laser) the data needs to be divided by the reference value (obtained in idle mode of the sensor) which allows us to get a fairly consistent value. As for finding the graph over time, we will proceed with:

- X axis: the value of (#ADCch/MIP)/(#ADCchMIP(t=0)), where #ADCchMIP(t=0) is the value set as gain at the beginning of sensors operation. In case of LHC, this would be a constant dependent on collision type (e. g. 16 for pp collisions), but for laser light this value is just the initial gain of the sensor at the beginning of the experiment (t=0);
- Y axis: at first we will proceed with the time of the experiment (in days), later we will however switch to using charge per area of the sensor, thus giving us a more reliable graph, showing actual usage of the sensor and not just the time of the experiment, which may not be a good indicator of the sensor's usage.

It should also be noted that data is available for different settings of magnetic field (0, 0.2T, 0.5T), at first we will proceed just with data for 0.5T. Data is available as ROOT files, potentially to be converted to CSV for initial data analysis. We will proceed with the following steps:

1. Data acquisition and potential conversion to CSV:
   - Get data as ROOT files
   - Write a ROOT routine to convert to CSV
2. EDA (exploratory data analysis) to understand the data and its structure
   - Use Python + Jupyter to explore the data
   - Look for missing values,
   - Look for potential patterns
   - Look for potential outliers
   - Look for potential correlations
3. Data preprocessing
   - Impute any missing values smartly
   - Create helper columns if necessary
   - Create helpers for later data analysis
   - Any other steps based on EDA
4. Offline analysis and graphing for the problem at hand
   - Use Python + Jupyter to create feature columns for the task
   - Use Python + Jupyter to create graphs for the task
5. Conversion to online mode
   - Convert Python + Jupyter to ROOT + C++ code
   - Write tasks for online mode that encompass the task
   - Write workflow for online mode
6. Testing of continous data transformation on STAGING run,

## Updated after user feedback

Task is now divided into two separate parts:

1. **Trending the ADC/MIP for calibration-related purposes:** This is based on the values obtained in PHYSICS runs and is purely related to creating trends over time for the ADC/MIP parameter, regarrdless of the used HV configuration. As such, this is going to be a QC task, with exact form of the graphs being a matter of consideration and based on user needs and user experience factors. It should be noted that we assume that in this case the ADC/MIP parameters **SHOULD NOT** be normalized to the first run, as it does not make sense. Rather, it should be shown around the reference line set at expected gain. It is also importnat to note that CTF data can be subsampled only for runs longer than half an hour, due to sensor heating and other factors. As such, full scope of this task can be tested only on live QC (possibly STAGING, but test QC should also give actionable results), due to data size constraints.
2. **Trending the ADC/MIP for ageing-related purposes**: In this case we need to find the trend that shows ACTUAL ageing, for the purpose of which we conduct LASER runs after beam dump. Important change is that due to spectre of light of the laser and thus possible missing bins in the histogram we should take the **weighted mean** instead of Gaussian fit for the target channels with the normalization to the simple average of fitted Gaussian means for each channel besides the noisy channel (209). Additionaly we need to account for **HV config** here, however due to predictable nature of impact of HV on the laser distribution we can assume this is possible to correct based on thresholding of consecutive HV config periods, since we assume that after HV correction the ADC/MIP parameter will be exactly equal to expected gain (14 for FT0). This should ideally be also created as a graph in QC, but the exact form remains unspecified. For the testing purposes we need to gather ALL of the data for FT0 laser runs, which will be accomplishing by loosening the filters progressively.

## Tasks with descriptions

### FV0 ADC/MIP per channel monitoring

This task is focused on providing ADC/MIP metrics for each channel at each run in Quality Control interface. These metrics are necessary for FV0 calibration and are already used in FT0. The inital approach is to apply fractional Gaussian fitting for each channel of the FV0 on the histogram created from the 5 minute sampling interval used by QC. We will try to implement this as `DigitQcTask` following the pattern:

- At each 5 minute interval build a histogram from the sampled values
- Fit a fractional Gaussian distribution, which is basically Gaussian distrbution but fitted only between `x_l` and `x_h` values, where `x_l = a * x_max`, `x_h = b * x_max`, `x_max` is the X-axis location of the maximum histogram bin and `a` and `b` are empirical parameters specific for rings and PMs.
- Normalize the mean and stddev of Gaussian fit to the desired gain, which is collision dependent (15 for pp collisions)
- Plot the mean and stddev at each channel in the QC, with reference line (which should be at 1)

**Update:**

Initially I implemented this task as a `DigitQc` task (working directly at each 5-minute sampling of the QC), but then, due to advice from *Andreas Molander* we modified it to work as `PostProcTask` which means it uses histograms created directly in the `DigitQcTask` and is performed in post processing, limiting the computational load. I will also add some basic checkers to automatically detect the quality based on the graph.

**Update v2:**

I implemented basic checkers:

- Quality *good* when error as compared to the reference line is not bigger than stddev for all channels
- Quality *moderate* when error (as described above) is bigger than stddev but only for some channels (exact number configurable in the workflow config)
- Quality *bad* otherwise

However, according to *Sahil Upadhaya,* the error needs to be not within stddev but rather smaller than `1/nCh`, where `nCh` is number of channels of subdetector. This however means that quality of ALL testing runs would be *bad* and as such, we have decided together to first talk with the end user (Varlan) before proceeding further.

### FT0 aging from LASER runs
