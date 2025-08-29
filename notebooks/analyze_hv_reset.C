// Clean ROOT macro for HV-corrected aging data (RESET method)
// Shows both polarities on one graph with proper dates
// Usage: root -l analyze_hv_reset.C

#include <iostream>
#include <vector>
#include <set>
#include <map>
#include <TFile.h>
#include <TTree.h>
#include <TGraph.h>
#include <TCanvas.h>
#include <TLine.h>
#include <TLatex.h>
#include <TLegend.h>
#include <TDatime.h>
#include <TAxis.h>
#include <TStyle.h>
#include <TMultiGraph.h>

void analyze_hv_reset() {
    gStyle->SetOptStat(0);
    gStyle->SetPadGridX(kTRUE);
    gStyle->SetPadGridY(kTRUE);

    TFile* f = new TFile("ft0_hv_aging_reset.root", "READ");
    if (!f || f->IsZombie()) {
        cout << "Error: Cannot open ft0_hv_aging_reset.root" << endl;
        return;
    }
    
    TTree* pos_tree = (TTree*)f->Get("aging_positive");
    TTree* neg_tree = (TTree*)f->Get("aging_negative");
    
    cout << "=== HV RESET ANALYSIS ===" << endl;
    if (pos_tree) cout << "Positive polarity: " << pos_tree->GetEntries() << " entries" << endl;
    if (neg_tree) cout << "Negative polarity: " << neg_tree->GetEntries() << " entries" << endl;
    
    TCanvas* c1 = new TCanvas("c1", "Channel 24 - HV Reset Analysis", 1400, 800);
    c1->SetMargin(0.1, 0.05, 0.12, 0.08);
    
    TMultiGraph* mg = new TMultiGraph();
    TLegend* legend = new TLegend(0.55, 0.80, 0.88, 0.65);
    
    int total_runs = 0;
    int total_hv_periods = 0;

    if (pos_tree) {
        pos_tree->Draw("hv_corrected_response:timestamp_unix:hv_period", "channel_id==24", "goff");
        int n_pos = pos_tree->GetSelectedRows();
        
        if (n_pos > 0) {
            vector<double> pos_times, pos_responses;
            set<int> pos_periods;
            
            for (int i = 0; i < n_pos; i++) {
                double time = pos_tree->GetV2()[i];
                double response = pos_tree->GetV1()[i];
                int hv_period = (int)pos_tree->GetV3()[i];
                
                pos_times.push_back(time);
                pos_responses.push_back(response);
                pos_periods.insert(hv_period);
            }
            
            TGraph* gr_pos = new TGraph(n_pos, &pos_times[0], &pos_responses[0]);
            gr_pos->SetMarkerStyle(20);
            gr_pos->SetMarkerSize(0.6);
            gr_pos->SetMarkerColor(kGreen+2);
            gr_pos->SetLineColor(kGreen+2);
            gr_pos->SetLineWidth(2);
            
            mg->Add(gr_pos);
            legend->AddEntry(gr_pos, Form("POSITIVE (%d runs, %d periods)", n_pos, (int)pos_periods.size()), "lp");
            
            total_runs += n_pos;
            total_hv_periods += pos_periods.size();
        }
    }
    
    if (neg_tree) {
        neg_tree->Draw("hv_corrected_response:timestamp_unix:hv_period", "channel_id==24", "goff");
        int n_neg = neg_tree->GetSelectedRows();
        
        if (n_neg > 0) {
            vector<double> neg_times, neg_responses;
            set<int> neg_periods;
            
            for (int i = 0; i < n_neg; i++) {
                double time = neg_tree->GetV2()[i];
                double response = neg_tree->GetV1()[i];
                int hv_period = (int)neg_tree->GetV3()[i];
                
                neg_times.push_back(time);
                neg_responses.push_back(response);
                neg_periods.insert(hv_period);
            }
            
            TGraph* gr_neg = new TGraph(n_neg, &neg_times[0], &neg_responses[0]);
            gr_neg->SetMarkerStyle(21);
            gr_neg->SetMarkerSize(0.6);
            gr_neg->SetMarkerColor(kRed);
            gr_neg->SetLineColor(kRed);
            gr_neg->SetLineWidth(2);
            
            mg->Add(gr_neg);
            legend->AddEntry(gr_neg, Form("NEGATIVE (%d runs, %d periods)", n_neg, (int)neg_periods.size()), "lp");
            
            total_runs += n_neg;
            total_hv_periods += neg_periods.size();
        }
    }

    mg->Draw("APL");
    mg->SetTitle("Channel 24 - HV Reset Analysis;Date;Normalized Response");

    mg->GetYaxis()->SetRangeUser(0.7, 1.3);
    mg->GetXaxis()->SetTimeDisplay(1);
    mg->GetXaxis()->SetTimeFormat("%Y-%m-%d");
    mg->GetXaxis()->SetTimeOffset(0, "gmt");
    mg->GetXaxis()->SetLabelSize(0.015);
    mg->GetXaxis()->SetLabelOffset(0.01);
    
    mg->GetYaxis()->SetTitle("HV-corrected response");
    mg->GetYaxis()->SetTitleSize(0.04);
    mg->GetYaxis()->SetLabelSize(0.035);
    
    TLine* baseline = new TLine(mg->GetXaxis()->GetXmin(), 1.0, mg->GetXaxis()->GetXmax(), 1.0);
    baseline->SetLineStyle(2);
    baseline->SetLineColor(kGray+1);
    baseline->SetLineWidth(2);
    baseline->Draw();
    
    if (pos_tree) {
        pos_tree->Draw("hv_corrected_response:timestamp_unix:hv_period", "channel_id==24", "goff");
        int n_pos = pos_tree->GetSelectedRows();
        
        if (n_pos > 0) {
            set<int> pos_periods;
            map<int, double> period_first_time;
            
            for (int i = 0; i < n_pos; i++) {
                double time = pos_tree->GetV2()[i];
                int hv_period = (int)pos_tree->GetV3()[i];
                
                if (pos_periods.find(hv_period) == pos_periods.end()) {
                    pos_periods.insert(hv_period);
                    period_first_time[hv_period] = time;
                }
            }
            
            for (int period : pos_periods) {
                TLine* line = new TLine(period_first_time[period], 0.7, period_first_time[period], 1.3);
                line->SetLineColor(kGreen+2);
                line->SetLineStyle(3);
                line->SetLineWidth(2);
                line->Draw();
            }
        }
    }

    if (neg_tree) {
        neg_tree->Draw("hv_corrected_response:timestamp_unix:hv_period", "channel_id==24", "goff");
        int n_neg = neg_tree->GetSelectedRows();
        
        if (n_neg > 0) {
            set<int> neg_periods;
            map<int, double> period_first_time;
            
            for (int i = 0; i < n_neg; i++) {
                double time = neg_tree->GetV2()[i];
                int hv_period = (int)neg_tree->GetV3()[i];
                
                if (neg_periods.find(hv_period) == neg_periods.end()) {
                    neg_periods.insert(hv_period);
                    period_first_time[hv_period] = time;
                }
            }

            for (int period : neg_periods) {
                TLine* line = new TLine(period_first_time[period], 0.7, period_first_time[period], 1.3);
                line->SetLineColor(kRed);
                line->SetLineStyle(3);
                line->SetLineWidth(2);
                line->Draw();
            }
        }
    }
    
    legend->SetBorderSize(1);
    legend->SetFillColor(kWhite);
    legend->SetTextSize(0.03);
    legend->Draw();
    
    TLatex* tex = new TLatex();
    tex->SetTextSize(0.025);
    tex->SetTextColor(kBlack);
    tex->DrawLatexNDC(0.15, 0.925, Form("Total: %d runs, %d HV periods", total_runs, total_hv_periods));
    
    c1->Modified();
    c1->Update();
    
    cout << "\nPlot created for Channel 24" << endl;
    cout << "Vertical lines mark HV configuration changes" << endl;
    cout << "Each HV period resets to 1.0 showing true correction effectiveness" << endl;

    cout << "\n=== Commands ===" << endl;
    cout << "For other channels: change \"channel_id==24\" to \"channel_id==X\" in the code" << endl;
    cout << "Data structure: aging_positive and aging_negative TTrees" << endl;
}