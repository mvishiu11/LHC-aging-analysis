
// ROOT macro to analyze HV-corrected aging data (RESET method)
// Each HV period starts at 1.0 - shows true HV correction effectiveness
// Usage: root -l analyze_hv_data.C

void analyze_hv_data() {
    // Open the ROOT file
    TFile* f = new TFile("ft0_hv_aging_reset.root", "READ");
    if (!f || f->IsZombie()) {
        cout << "Error: Cannot open ft0_hv_aging_reset.root" << endl;
        return;
    }
    
    // Get the TTrees
    TTree* pos_tree = (TTree*)f->Get("aging_positive");
    TTree* neg_tree = (TTree*)f->Get("aging_negative");
    
    if (pos_tree) cout << "Positive polarity data: " << pos_tree->GetEntries() << " entries" << endl;
    if (neg_tree) cout << "Negative polarity data: " << neg_tree->GetEntries() << " entries" << endl;
    
    // Example: Plot channel 24 positive polarity (RESET approach)
    if (pos_tree) {
        TCanvas* c1 = new TCanvas("c1", "Channel 24 HV Reset Analysis", 1600, 1000);
        
        // Create graph for channel 24 positive polarity
        pos_tree->Draw("hv_corrected_response:timestamp_unix", "channel_id==24", "goff");
        
        int n_points = pos_tree->GetSelectedRows();
        TGraph* gr = new TGraph(n_points, pos_tree->GetV2(), pos_tree->GetV1());
        
        gr->SetTitle("Channel 24 Positive - HV Reset Analysis (Each HV Period Starts at 1.0);Time;Normalized Response");
        gr->SetMarkerStyle(20);
        gr->SetMarkerSize(0.8);
        gr->SetMarkerColor(kGreen+2);
        gr->SetLineColor(kGreen+2);
        gr->SetLineWidth(2);
        
        gr->Draw("APL");
        
        // Add horizontal line at y=1.0
        TLine* line = new TLine(gr->GetXaxis()->GetXmin(), 1.0, gr->GetXaxis()->GetXmax(), 1.0);
        line->SetLineStyle(2);
        line->SetLineColor(kGray+2);
        line->SetLineWidth(2);
        line->Draw();
        
        c1->Modified();
        c1->Update();
    }
    
    // Print some statistics
    cout << "\n=== RESET METHOD ANALYSIS ===" << endl;
    cout << "Each HV configuration period starts at 1.0" << endl;
    cout << "This shows the true effectiveness of HV corrections" << endl;
    cout << "\nTo plot other channels, use:" << endl;
    cout << "aging_positive->Draw(\"hv_corrected_response:timestamp_unix\", \"channel_id==X\", \"APL\");" << endl;
    cout << "Where X is the channel number (0-207)" << endl;
    
    // Show HV periods for channel 24
    if (pos_tree) {
        cout << "\nHV periods for channel 24 (positive):" << endl;
        pos_tree->Draw("hv_period", "channel_id==24", "goff");
        
        // Count unique HV periods
        set<int> hv_periods;
        for (int i = 0; i < pos_tree->GetSelectedRows(); i++) {
            hv_periods.insert((int)pos_tree->GetV1()[i]);
        }
        
        cout << "Number of HV periods: " << hv_periods.size() << endl;
        cout << "HV period indices: ";
        for (auto period : hv_periods) {
            cout << period << " ";
        }
        cout << endl;
    }
}
