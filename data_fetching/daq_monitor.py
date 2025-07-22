#!/usr/bin/env python3
"""
Ultra-Modern CERN System Monitor
Advanced system monitoring with particle physics flair
"""

import tkinter as tk
from tkinter import ttk
import psutil
import threading
import time
import os
import socket
import json
import queue
from collections import deque
from datetime import datetime
import math
import random

class ModernCERNMonitor:
    def __init__(self, root):
        self.root = root
        self.root.title("◆ CERN System Monitor ◆")
        self.root.geometry("1400x900")
        self.root.configure(bg='#0a0a0a')
        
        # Data persistence
        self.data_dir = os.path.expanduser("~/.cern_monitor")
        self.jobs_file = os.path.join(self.data_dir, "lhc_jobs.json")
        self.ensure_data_dir()
        
        # Monitoring flags
        self.monitoring = True
        self.current_tab = 0
        
        # Data storage for metrics
        self.cpu_history = deque(maxlen=120)  # 2 minutes of data
        self.ram_history = deque(maxlen=120)
        self.net_history = deque(maxlen=120)
        self.temp_history = deque(maxlen=120)
        
        # Job-specific resource tracking
        self.job_cpu_history = deque(maxlen=300)  # 5 minutes for jobs
        self.job_ram_history = deque(maxlen=300)
        self.job_net_history = deque(maxlen=300)
        self.job_progress_history = deque(maxlen=300)
        
        # Network counters
        self.last_net_io = psutil.net_io_counters()
        self.last_disk_io = psutil.disk_io_counters()
        self.last_time = time.time()
        
        # Process communication
        self.process_queue = queue.Queue()
        self.external_process_data = {
            'name': 'No Active Collision',
            'status': 'Idle',
            'progress': 0,
            'details': {},
            'logs': []
        }
        
        # Jobs database
        self.jobs_history = self.load_jobs_history()
        self.current_job_id = None
        self.current_job_data = {}
        self.job_start_resources = {}
        
        # CERN Easter eggs
        self.particles = ['⚛️', '🔬', '⚡', '💫', '🌌']
        self.physics_constants = {
            'c': '299,792,458 m/s',
            'ℏ': '1.055×10⁻³⁴ J·s',
            'e': '1.602×10⁻¹⁹ C',
            'mₚ': '938.3 MeV/c²'
        }
        self.lhc_energies = ['13 TeV', '14 TeV', '6.5 TeV/beam']
        
        # Modern color scheme with physics flair
        self.colors = {
            'bg': '#0a0a0a',
            'surface': '#1a1a1a',
            'surface_light': '#2a2a2a',
            'surface_hover': '#3a3a3a',
            'primary': '#00d4ff',      # CERN blue
            'secondary': '#ff6b35',    # LHC orange
            'accent': '#00ff88',       # Particle green
            'warning': '#ffaa00',      # Caution yellow
            'error': '#ff4757',        # Alert red
            'success': '#2ed573',      # Success green
            'text_primary': '#ffffff',
            'text_secondary': '#b8b8b8',
            'text_dim': '#6b6b6b',
            'border': '#333333',
            'glow': '#00d4ff',
            'particle': '#ff6b35',
            'higgs': '#9b59b6'
        }
        
        self.setup_modern_styles()
        self.setup_ui()
        self.start_monitoring()
        self.start_process_listener()
        self.start_particle_animation()
        
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
    
    def ensure_data_dir(self):
        """Ensure data directory exists"""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def load_jobs_history(self):
        """Load LHC jobs history"""
        try:
            if os.path.exists(self.jobs_file):
                with open(self.jobs_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            print(f"Error loading collision data: {e}")
        return {}
    
    def save_jobs_history(self):
        """Save collision data to storage"""
        try:
            with open(self.jobs_file, 'w') as f:
                json.dump(self.jobs_history, f, indent=2)
        except Exception as e:
            print(f"Error saving collision data: {e}")
    
    def setup_modern_styles(self):
        """Configure ultra-modern theme"""
        style = ttk.Style()
        style.theme_use('clam')
        
        # Configure modern styles with gradients
        style.configure('Modern.TFrame',
                       background=self.colors['surface'],
                       borderwidth=0,
                       relief='flat')
        
        style.configure('Glow.TFrame',
                       background=self.colors['surface'],
                       borderwidth=1,
                       relief='solid',
                       bordercolor=self.colors['primary'])
        
        # Treeview with modern look
        style.configure('Modern.Treeview',
                       background=self.colors['surface'],
                       foreground=self.colors['text_primary'],
                       fieldbackground=self.colors['surface'],
                       borderwidth=0,
                       rowheight=30)
        
        style.configure('Modern.Treeview.Heading',
                       background=self.colors['surface_light'],
                       foreground=self.colors['text_primary'],
                       font=('SF Pro Display', 11, 'bold'),
                       borderwidth=1,
                       relief='solid')
        
        style.map('Modern.Treeview',
                 background=[('selected', self.colors['primary'])],
                 foreground=[('selected', self.colors['bg'])])
    
    def setup_ui(self):
        """Setup ultra-modern UI"""
        # Main container
        main_container = tk.Frame(self.root, bg=self.colors['bg'])
        main_container.pack(fill=tk.BOTH, expand=True)
        
        # Header with particle effects
        self.create_modern_header(main_container)
        
        # Custom modern tabs
        self.create_modern_tabs(main_container)
        
        # Tab content areas
        self.tab_content = tk.Frame(main_container, bg=self.colors['bg'])
        self.tab_content.pack(fill=tk.BOTH, expand=True, padx=20, pady=(0, 20))
        
        # Create tab content
        self.create_system_content()
        self.create_jobs_content()
        
        # Show initial tab
        self.show_tab(0)
    
    def create_modern_header(self, parent):
        """Create modern header with particle effects"""
        header_frame = tk.Frame(parent, bg=self.colors['bg'], height=80)
        header_frame.pack(fill=tk.X, padx=20, pady=20)
        header_frame.pack_propagate(False)
        
        # Left side - Title with physics flair
        left_frame = tk.Frame(header_frame, bg=self.colors['bg'])
        left_frame.pack(side=tk.LEFT, fill=tk.Y)
        
        title_frame = tk.Frame(left_frame, bg=self.colors['bg'])
        title_frame.pack(anchor='w')
        
        # Animated particle before title
        self.particle_label = tk.Label(title_frame, 
                                      text=random.choice(self.particles),
                                      bg=self.colors['bg'], 
                                      fg=self.colors['particle'],
                                      font=('SF Pro Display', 20))
        self.particle_label.pack(side=tk.LEFT, padx=(0, 10))
        
        title_label = tk.Label(title_frame, 
                              text="CERN Control Center",
                              bg=self.colors['bg'], 
                              fg=self.colors['text_primary'],
                              font=('SF Pro Display', 26, 'bold'))
        title_label.pack(side=tk.LEFT)
        
        # Subtitle with physics reference
        subtitle_label = tk.Label(left_frame,
                                 text="Data Acquistion Monitoring",
                                 bg=self.colors['bg'],
                                 fg=self.colors['text_secondary'],
                                 font=('SF Pro Display', 11))
        subtitle_label.pack(anchor='w', pady=(2, 0))
        
        # Right side - Status and time
        right_frame = tk.Frame(header_frame, bg=self.colors['bg'])
        right_frame.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Time with physics constant easter egg
        time_frame = tk.Frame(right_frame, bg=self.colors['bg'])
        time_frame.pack(anchor='e')
        
        self.time_label = tk.Label(time_frame,
                                  text="",
                                  bg=self.colors['bg'],
                                  fg=self.colors['text_primary'],
                                  font=('SF Mono', 16, 'bold'))
        self.time_label.pack(side=tk.LEFT)
        
        # Random physics constant display
        self.physics_label = tk.Label(time_frame,
                                     text="",
                                     bg=self.colors['bg'],
                                     fg=self.colors['text_dim'],
                                     font=('SF Mono', 9))
        self.physics_label.pack(side=tk.LEFT, padx=(20, 0))
        
        # Status with beam energy
        status_frame = tk.Frame(right_frame, bg=self.colors['bg'])
        status_frame.pack(anchor='e', pady=(5, 0))
        
        # Beam status dot
        self.beam_dot = tk.Canvas(status_frame, width=12, height=12,
                                 bg=self.colors['bg'], highlightthickness=0)
        self.beam_dot.pack(side=tk.LEFT, padx=(0, 8))
        self.beam_dot.create_oval(2, 2, 10, 10, fill=self.colors['success'], outline="")
        
        self.status_label = tk.Label(status_frame,
                                    text="DAQ Stable • All parameters normal",
                                    bg=self.colors['bg'],
                                    fg=self.colors['text_secondary'],
                                    font=('SF Pro Display', 11))
        self.status_label.pack(side=tk.LEFT)
    
    def create_modern_tabs(self, parent):
        """Create custom modern tabs"""
        tabs_container = tk.Frame(parent, bg=self.colors['bg'], height=60)
        tabs_container.pack(fill=tk.X, padx=20, pady=(0, 20))
        tabs_container.pack_propagate(False)
        
        # Tab buttons container
        tabs_frame = tk.Frame(tabs_container, bg=self.colors['bg'])
        tabs_frame.pack(expand=True)
        
        self.tab_buttons = []
        
        # System Monitor tab
        system_tab = self.create_tab_button(tabs_frame, "🖥️ System Monitor", 0,
                                           "Real-time system diagnostics")
        system_tab.pack(side=tk.LEFT, padx=(0, 5))
        self.tab_buttons.append(system_tab)
        
        # Collisions Database tab
        jobs_tab = self.create_tab_button(tabs_frame, "⚛️ Data Acquisition Database", 1,
                                         "LHC data acquisition jobs")
        jobs_tab.pack(side=tk.LEFT, padx=5)
        self.tab_buttons.append(jobs_tab)
        
        # Update tab appearance
        self.update_tab_buttons()
    
    def create_tab_button(self, parent, text, index, tooltip):
        """Create a modern tab button"""
        # Tab container with hover effects
        tab_container = tk.Frame(parent, bg=self.colors['surface'], relief='flat')
        
        # Tab button
        tab_button = tk.Button(tab_container,
                              text=text,
                              command=lambda: self.show_tab(index),
                              bg=self.colors['surface'],
                              fg=self.colors['text_secondary'],
                              font=('SF Pro Display', 12, 'bold'),
                              relief='flat',
                              bd=0,
                              padx=25,
                              pady=12,
                              cursor='hand2')
        tab_button.pack()
        
        # Hover effects
        def on_enter(e):
            if index != self.current_tab:
                tab_button.config(bg=self.colors['surface_hover'],
                                 fg=self.colors['text_primary'])
        
        def on_leave(e):
            if index != self.current_tab:
                tab_button.config(bg=self.colors['surface'],
                                 fg=self.colors['text_secondary'])
        
        tab_button.bind("<Enter>", on_enter)
        tab_button.bind("<Leave>", on_leave)
        
        # Store button reference
        tab_container.button = tab_button
        tab_container.index = index
        
        return tab_container
    
    def update_tab_buttons(self):
        """Update tab button appearances"""
        for i, tab_container in enumerate(self.tab_buttons):
            button = tab_container.button
            if i == self.current_tab:
                button.config(bg=self.colors['primary'],
                             fg=self.colors['bg'])
                # Add glow effect
                tab_container.config(bg=self.colors['primary'])
            else:
                button.config(bg=self.colors['surface'],
                             fg=self.colors['text_secondary'])
                tab_container.config(bg=self.colors['surface'])
    
    def show_tab(self, index):
        """Show specific tab content"""
        self.current_tab = index
        self.update_tab_buttons()
        
        # Hide all content
        for widget in self.tab_content.winfo_children():
            widget.pack_forget()
        
        # Show selected content
        if index == 0:
            self.system_content.pack(fill=tk.BOTH, expand=True)
        elif index == 1:
            self.jobs_content.pack(fill=tk.BOTH, expand=True)
    
    def create_system_content(self):
        """Create system monitoring content"""
        self.system_content = tk.Frame(self.tab_content, bg=self.colors['bg'])
        
        # Configure grid
        self.system_content.columnconfigure(0, weight=1)
        self.system_content.columnconfigure(1, weight=1)
        self.system_content.columnconfigure(2, weight=1)
        self.system_content.rowconfigure(0, weight=1)
        self.system_content.rowconfigure(1, weight=1)
        self.system_content.rowconfigure(2, weight=1)
        
        # System metrics cards
        self.create_cpu_card(self.system_content, 0, 0)
        self.create_memory_card(self.system_content, 0, 1)
        self.create_network_card(self.system_content, 0, 2)
        
        self.create_storage_card(self.system_content, 1, 0)
        self.create_process_card(self.system_content, 1, 1)
        self.create_system_card(self.system_content, 1, 2)
        
        # Enhanced job monitor
        self.create_enhanced_job_card(self.system_content, 2, 0, 3)
    
    def create_jobs_content(self):
        """Create jobs database content"""
        self.jobs_content = tk.Frame(self.tab_content, bg=self.colors['bg'])
        
        # Top stats row
        stats_frame = tk.Frame(self.jobs_content, bg=self.colors['bg'])
        stats_frame.pack(fill=tk.X, pady=(0, 20))
        
        stats_frame.columnconfigure(0, weight=1)
        stats_frame.columnconfigure(1, weight=1)
        stats_frame.columnconfigure(2, weight=1)
        stats_frame.columnconfigure(3, weight=1)
        
        self.create_collision_stats_card(stats_frame, 0, 0)
        self.create_active_collision_card(stats_frame, 0, 1)
        self.create_beam_stats_card(stats_frame, 0, 2)
        self.create_recent_events_card(stats_frame, 0, 3)
        
        # Enhanced jobs table
        self.create_enhanced_jobs_table(self.jobs_content)
    
    def create_modern_card(self, parent, row, col, rowspan=1, colspan=1, glow=False):
        """Create ultra-modern card with glow effects"""
        # Outer glow container
        if glow:
            glow_container = tk.Frame(parent, bg=self.colors['glow'])
            glow_container.grid(row=row, column=col, rowspan=rowspan, columnspan=colspan,
                               sticky='nsew', padx=12, pady=12)
            
            # Main card
            card = tk.Frame(glow_container, bg=self.colors['surface'], relief='flat')
            card.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)
        else:
            card = tk.Frame(parent, bg=self.colors['surface'], relief='flat')
            card.grid(row=row, column=col, rowspan=rowspan, columnspan=colspan,
                     sticky='nsew', padx=12, pady=12)
        
        # Top accent line
        accent_line = tk.Frame(card, bg=self.colors['primary'], height=2)
        accent_line.pack(fill=tk.X, side=tk.TOP)
        
        return card
    
    def create_cpu_card(self, parent, row, col):
        """Enhanced CPU card with particle theme"""
        card = self.create_modern_card(parent, row, col)
        
        # Header
        header_frame = tk.Frame(card, bg=self.colors['surface'])
        header_frame.pack(fill=tk.X, padx=20, pady=(15, 10))
        
        tk.Label(header_frame, text="⚡ CPU Usage",
                bg=self.colors['surface'], fg=self.colors['text_primary'],
                font=('SF Pro Display', 13, 'bold')).pack(side=tk.LEFT)
        
        # CPU percentage with large display
        self.cpu_metric = tk.Label(card, text="0%",
                                  bg=self.colors['surface'], fg=self.colors['primary'],
                                  font=('SF Mono', 28, 'bold'))
        self.cpu_metric.pack(pady=(0, 10))
        
        # Modern progress bar
        progress_frame = tk.Frame(card, bg=self.colors['surface'], height=8)
        progress_frame.pack(fill=tk.X, padx=20, pady=(0, 15))
        progress_frame.pack_propagate(False)
        
        self.cpu_bg_bar = tk.Frame(progress_frame, bg=self.colors['bg'], height=8)
        self.cpu_bg_bar.pack(fill=tk.X)
        
        self.cpu_progress_bar = tk.Frame(self.cpu_bg_bar, bg=self.colors['primary'], height=8)
        self.cpu_progress_bar.place(x=0, y=0, height=8, width=0)
        
        # Details with physics flair
        details_frame = tk.Frame(card, bg=self.colors['surface'])
        details_frame.pack(fill=tk.X, padx=20, pady=(0, 15))
        
        self.cpu_cores_label = tk.Label(details_frame, text="",
                                       bg=self.colors['surface'], fg=self.colors['text_secondary'],
                                       font=('SF Pro Display', 10))
        self.cpu_cores_label.pack(anchor=tk.W)
        
        self.cpu_freq_label = tk.Label(details_frame, text="",
                                      bg=self.colors['surface'], fg=self.colors['text_secondary'],
                                      font=('SF Pro Display', 10))
        self.cpu_freq_label.pack(anchor=tk.W)
        
        # Enhanced graph
        self.cpu_graph = tk.Canvas(card, height=50, bg=self.colors['surface'],
                                  highlightthickness=0)
        self.cpu_graph.pack(fill=tk.X, padx=20, pady=(0, 15))
    
    def create_memory_card(self, parent, row, col):
        """Enhanced memory card"""
        card = self.create_modern_card(parent, row, col)
        
        header_frame = tk.Frame(card, bg=self.colors['surface'])
        header_frame.pack(fill=tk.X, padx=20, pady=(15, 10))
        
        tk.Label(header_frame, text="🧠 RAM Usage",
                bg=self.colors['surface'], fg=self.colors['text_primary'],
                font=('SF Pro Display', 13, 'bold')).pack(side=tk.LEFT)
        
        self.memory_metric = tk.Label(card, text="0%",
                                     bg=self.colors['surface'], fg=self.colors['accent'],
                                     font=('SF Mono', 28, 'bold'))
        self.memory_metric.pack(pady=(0, 10))
        
        # Progress bar
        progress_frame = tk.Frame(card, bg=self.colors['surface'], height=8)
        progress_frame.pack(fill=tk.X, padx=20, pady=(0, 15))
        progress_frame.pack_propagate(False)
        
        self.memory_bg_bar = tk.Frame(progress_frame, bg=self.colors['bg'], height=8)
        self.memory_bg_bar.pack(fill=tk.X)
        
        self.memory_progress_bar = tk.Frame(self.memory_bg_bar, bg=self.colors['accent'], height=8)
        self.memory_progress_bar.place(x=0, y=0, height=8, width=0)
        
        # Details
        details_frame = tk.Frame(card, bg=self.colors['surface'])
        details_frame.pack(fill=tk.X, padx=20, pady=(0, 15))
        
        self.memory_used_label = tk.Label(details_frame, text="",
                                         bg=self.colors['surface'], fg=self.colors['text_secondary'],
                                         font=('SF Pro Display', 10))
        self.memory_used_label.pack(anchor=tk.W)
        
        self.memory_available_label = tk.Label(details_frame, text="",
                                              bg=self.colors['surface'], fg=self.colors['text_secondary'],
                                              font=('SF Pro Display', 10))
        self.memory_available_label.pack(anchor=tk.W)
        
        # Graph
        self.memory_graph = tk.Canvas(card, height=50, bg=self.colors['surface'],
                                     highlightthickness=0)
        self.memory_graph.pack(fill=tk.X, padx=20, pady=(0, 15))
    
    def create_network_card(self, parent, row, col):
        """Enhanced network card"""
        card = self.create_modern_card(parent, row, col)
        
        header_frame = tk.Frame(card, bg=self.colors['surface'])
        header_frame.pack(fill=tk.X, padx=20, pady=(15, 10))
        
        tk.Label(header_frame, text="🌐 Network Usage",
                bg=self.colors['surface'], fg=self.colors['text_primary'],
                font=('SF Pro Display', 13, 'bold')).pack(side=tk.LEFT)
        
        self.network_metric = tk.Label(card, text="0 KB/s",
                                      bg=self.colors['surface'], fg=self.colors['secondary'],
                                      font=('SF Mono', 20, 'bold'))
        self.network_metric.pack(pady=(0, 10))
        
        # Upload/Download with physics units
        speeds_frame = tk.Frame(card, bg=self.colors['surface'])
        speeds_frame.pack(pady=(0, 15))
        
        self.upload_label = tk.Label(speeds_frame, text="↑ 0 KB/s",
                                    bg=self.colors['surface'], fg=self.colors['text_secondary'],
                                    font=('SF Pro Display', 10))
        self.upload_label.pack()
        
        self.download_label = tk.Label(speeds_frame, text="↓ 0 KB/s",
                                      bg=self.colors['surface'], fg=self.colors['text_secondary'],
                                      font=('SF Pro Display', 10))
        self.download_label.pack()
        
        # Graph
        self.network_graph = tk.Canvas(card, height=50, bg=self.colors['surface'],
                                      highlightthickness=0)
        self.network_graph.pack(fill=tk.X, padx=20, pady=(0, 15))
    
    def create_storage_card(self, parent, row, col):
        """Enhanced storage card"""
        card = self.create_modern_card(parent, row, col)
        
        header_frame = tk.Frame(card, bg=self.colors['surface'])
        header_frame.pack(fill=tk.X, padx=20, pady=(15, 10))
        
        tk.Label(header_frame, text="💾 Storage",
                bg=self.colors['surface'], fg=self.colors['text_primary'],
                font=('SF Pro Display', 13, 'bold')).pack(side=tk.LEFT)
        
        self.storage_metric = tk.Label(card, text="0%",
                                      bg=self.colors['surface'], fg=self.colors['warning'],
                                      font=('SF Mono', 28, 'bold'))
        self.storage_metric.pack(pady=(0, 10))
        
        # Progress bar
        progress_frame = tk.Frame(card, bg=self.colors['surface'], height=8)
        progress_frame.pack(fill=tk.X, padx=20, pady=(0, 15))
        progress_frame.pack_propagate(False)
        
        self.storage_bg_bar = tk.Frame(progress_frame, bg=self.colors['bg'], height=8)
        self.storage_bg_bar.pack(fill=tk.X)
        
        self.storage_progress_bar = tk.Frame(self.storage_bg_bar, bg=self.colors['warning'], height=8)
        self.storage_progress_bar.place(x=0, y=0, height=8, width=0)
        
        # Details
        details_frame = tk.Frame(card, bg=self.colors['surface'])
        details_frame.pack(fill=tk.X, padx=20, pady=(0, 15))
        
        self.storage_used_label = tk.Label(details_frame, text="",
                                          bg=self.colors['surface'], fg=self.colors['text_secondary'],
                                          font=('SF Pro Display', 10))
        self.storage_used_label.pack(anchor=tk.W)
        
        self.storage_free_label = tk.Label(details_frame, text="",
                                          bg=self.colors['surface'], fg=self.colors['text_secondary'],
                                          font=('SF Pro Display', 10))
        self.storage_free_label.pack(anchor=tk.W)
    
    def create_process_card(self, parent, row, col):
        """Enhanced process card"""
        card = self.create_modern_card(parent, row, col)
        
        header_frame = tk.Frame(card, bg=self.colors['surface'])
        header_frame.pack(fill=tk.X, padx=20, pady=(15, 10))
        
        tk.Label(header_frame, text="⚙️ Active Processes",
                bg=self.colors['surface'], fg=self.colors['text_primary'],
                font=('SF Pro Display', 13, 'bold')).pack(side=tk.LEFT)
        
        self.process_count_metric = tk.Label(card, text="0",
                                            bg=self.colors['surface'], fg=self.colors['success'],
                                            font=('SF Mono', 28, 'bold'))
        self.process_count_metric.pack(pady=(0, 5))
        
        tk.Label(card, text="Running Processes",
                bg=self.colors['surface'], fg=self.colors['text_secondary'],
                font=('SF Pro Display', 10)).pack(pady=(0, 15))
        
        # Top processes
        processes_frame = tk.Frame(card, bg=self.colors['surface'])
        processes_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=(0, 15))
        
        self.process_labels = []
        for i in range(6):
            label = tk.Label(processes_frame, text="",
                           bg=self.colors['surface'], fg=self.colors['text_secondary'],
                           font=('SF Mono', 8))
            label.pack(anchor=tk.W, pady=1)
            self.process_labels.append(label)
    
    def create_system_card(self, parent, row, col):
        """Enhanced system info card"""
        card = self.create_modern_card(parent, row, col)
        
        header_frame = tk.Frame(card, bg=self.colors['surface'])
        header_frame.pack(fill=tk.X, padx=20, pady=(15, 10))
        
        tk.Label(header_frame, text="🔬 System Core",
                bg=self.colors['surface'], fg=self.colors['text_primary'],
                font=('SF Pro Display', 13, 'bold')).pack(side=tk.LEFT)
        
        # Uptime with scientific format
        self.uptime_label = tk.Label(card, text="",
                                    bg=self.colors['surface'], fg=self.colors['primary'],
                                    font=('SF Mono', 18, 'bold'))
        self.uptime_label.pack(pady=(0, 5))
        
        tk.Label(card, text="System Uptime",
                bg=self.colors['surface'], fg=self.colors['text_secondary'],
                font=('SF Pro Display', 10)).pack(pady=(0, 15))
        
        # System details
        details_frame = tk.Frame(card, bg=self.colors['surface'])
        details_frame.pack(fill=tk.X, padx=20, pady=(0, 15))
        
        self.hostname_label = tk.Label(details_frame, text="",
                                      bg=self.colors['surface'], fg=self.colors['text_secondary'],
                                      font=('SF Pro Display', 9))
        self.hostname_label.pack(anchor=tk.W, pady=1)
        
        self.temperature_label = tk.Label(details_frame, text="",
                                         bg=self.colors['surface'], fg=self.colors['text_secondary'],
                                         font=('SF Pro Display', 9))
        self.temperature_label.pack(anchor=tk.W, pady=1)
        
        self.load_label = tk.Label(details_frame, text="",
                                  bg=self.colors['surface'], fg=self.colors['text_secondary'],
                                  font=('SF Pro Display', 9))
        self.load_label.pack(anchor=tk.W, pady=1)
    
    def create_enhanced_job_card(self, parent, row, col, colspan=1):
        """Enhanced job monitoring with real-time graphs"""
        card = self.create_modern_card(parent, row, col, colspan=colspan, glow=True)
        
        # Header with status
        header_frame = tk.Frame(card, bg=self.colors['surface'])
        header_frame.pack(fill=tk.X, padx=20, pady=(15, 10))
        
        tk.Label(header_frame, text="⚛️ DAQ Jobs Monitor",
                bg=self.colors['surface'], fg=self.colors['text_primary'],
                font=('SF Pro Display', 14, 'bold')).pack(side=tk.LEFT)
        
        # Beam status indicator
        self.collision_dot = tk.Canvas(header_frame, width=12, height=12,
                                      bg=self.colors['surface'], highlightthickness=0)
        self.collision_dot.pack(side=tk.RIGHT)
        self.collision_dot.create_oval(1, 1, 11, 11, fill=self.colors['border'], outline="")
        
        # Main content area
        content_area = tk.Frame(card, bg=self.colors['surface'])
        content_area.pack(fill=tk.BOTH, expand=True, padx=20, pady=(0, 15))
        
        # Left side - job info
        left_frame = tk.Frame(content_area, bg=self.colors['surface'])
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        self.job_name_label = tk.Label(left_frame, text="No Active Job",
                                      bg=self.colors['surface'], fg=self.colors['text_primary'],
                                      font=('SF Pro Display', 16, 'bold'))
        self.job_name_label.pack(anchor=tk.W)
        
        self.job_status_label = tk.Label(left_frame, text="Standby Mode",
                                        bg=self.colors['surface'], fg=self.colors['text_secondary'],
                                        font=('SF Pro Display', 12))
        self.job_status_label.pack(anchor=tk.W, pady=(2, 15))
        
        # Enhanced progress with particle animation
        progress_container = tk.Frame(left_frame, bg=self.colors['surface'])
        progress_container.pack(fill=tk.X, pady=(0, 10))
        
        self.job_progress_bg = tk.Frame(progress_container, bg=self.colors['bg'], height=8)
        self.job_progress_bg.pack(fill=tk.X)
        
        self.job_progress_bar = tk.Frame(self.job_progress_bg, bg=self.colors['primary'], height=8)
        self.job_progress_bar.place(x=0, y=0, height=8, width=0)
        
        self.job_progress_label = tk.Label(left_frame, text="0.0%",
                                          bg=self.colors['surface'], fg=self.colors['text_secondary'],
                                          font=('SF Pro Display', 10))
        self.job_progress_label.pack(anchor=tk.W)
        
        # Job metrics
        metrics_frame = tk.Frame(left_frame, bg=self.colors['surface'])
        metrics_frame.pack(fill=tk.X, pady=(15, 0))
        
        self.job_completed_label = tk.Label(metrics_frame, text="Events: 0",
                                           bg=self.colors['surface'], fg=self.colors['success'],
                                           font=('SF Pro Display', 10))
        self.job_completed_label.pack(anchor=tk.W, pady=1)
        
        self.job_failed_label = tk.Label(metrics_frame, text="Errors: 0",
                                        bg=self.colors['surface'], fg=self.colors['error'],
                                        font=('SF Pro Display', 10))
        self.job_failed_label.pack(anchor=tk.W, pady=1)
        
        self.job_eta_label = tk.Label(metrics_frame, text="ETA: ∞",
                                     bg=self.colors['surface'], fg=self.colors['accent'],
                                     font=('SF Pro Display', 10))
        self.job_eta_label.pack(anchor=tk.W, pady=1)
        
        # Right side - real-time resource graphs
        right_frame = tk.Frame(content_area, bg=self.colors['surface'])
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=(20, 0))
        
        tk.Label(right_frame, text="Resource Usage During Collision",
                bg=self.colors['surface'], fg=self.colors['text_primary'],
                font=('SF Pro Display', 12, 'bold')).pack(anchor=tk.W)
        
        # Resource graphs container
        graphs_container = tk.Frame(right_frame, bg=self.colors['surface'])
        graphs_container.pack(fill=tk.BOTH, expand=True, pady=(10, 0))
        
        # CPU graph for job
        cpu_graph_frame = tk.Frame(graphs_container, bg=self.colors['surface'])
        cpu_graph_frame.pack(fill=tk.X, pady=(0, 8))
        
        tk.Label(cpu_graph_frame, text="CPU %",
                bg=self.colors['surface'], fg=self.colors['primary'],
                font=('SF Pro Display', 9, 'bold')).pack(anchor=tk.W)
        
        self.job_cpu_graph = tk.Canvas(cpu_graph_frame, height=40, bg=self.colors['surface'],
                                      highlightthickness=0)
        self.job_cpu_graph.pack(fill=tk.X, pady=(2, 0))
        
        # Memory graph for job
        mem_graph_frame = tk.Frame(graphs_container, bg=self.colors['surface'])
        mem_graph_frame.pack(fill=tk.X, pady=(0, 8))
        
        tk.Label(mem_graph_frame, text="Memory %",
                bg=self.colors['surface'], fg=self.colors['accent'],
                font=('SF Pro Display', 9, 'bold')).pack(anchor=tk.W)
        
        self.job_mem_graph = tk.Canvas(mem_graph_frame, height=40, bg=self.colors['surface'],
                                      highlightthickness=0)
        self.job_mem_graph.pack(fill=tk.X, pady=(2, 0))
        
        # Progress timeline
        progress_graph_frame = tk.Frame(graphs_container, bg=self.colors['surface'])
        progress_graph_frame.pack(fill=tk.X)
        
        tk.Label(progress_graph_frame, text="Progress Timeline",
                bg=self.colors['surface'], fg=self.colors['secondary'],
                font=('SF Pro Display', 9, 'bold')).pack(anchor=tk.W)
        
        self.job_progress_graph = tk.Canvas(progress_graph_frame, height=40, bg=self.colors['surface'],
                                           highlightthickness=0)
        self.job_progress_graph.pack(fill=tk.X, pady=(2, 0))
    
    def create_collision_stats_card(self, parent, row, col):
        """Collision statistics card"""
        card = self.create_modern_card(parent, row, col)
        
        tk.Label(card, text="⚛️ DAQ Jobs Statistics",
                bg=self.colors['surface'], fg=self.colors['text_primary'],
                font=('SF Pro Display', 12, 'bold')).pack(pady=(15, 10))
        
        stats_frame = tk.Frame(card, bg=self.colors['surface'])
        stats_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=(0, 15))
        
        self.total_collisions_label = tk.Label(stats_frame, text="Total Runs: 0",
                                              bg=self.colors['surface'], fg=self.colors['text_secondary'],
                                              font=('SF Pro Display', 10))
        self.total_collisions_label.pack(anchor=tk.W, pady=2)
        
        self.successful_collisions_label = tk.Label(stats_frame, text="Successful: 0",
                                                   bg=self.colors['surface'], fg=self.colors['success'],
                                                   font=('SF Pro Display', 10))
        self.successful_collisions_label.pack(anchor=tk.W, pady=2)
        
        self.failed_collisions_label = tk.Label(stats_frame, text="Failed: 0",
                                               bg=self.colors['surface'], fg=self.colors['error'],
                                               font=('SF Pro Display', 10))
        self.failed_collisions_label.pack(anchor=tk.W, pady=2)
        
        self.efficiency_label = tk.Label(stats_frame, text="Efficiency: 0%",
                                        bg=self.colors['surface'], fg=self.colors['primary'],
                                        font=('SF Pro Display', 10, 'bold'))
        self.efficiency_label.pack(anchor=tk.W, pady=2)
    
    def create_active_collision_card(self, parent, row, col):
        """Active collision card"""
        card = self.create_modern_card(parent, row, col)
        
        tk.Label(card, text="🔄 Active Job",
                bg=self.colors['surface'], fg=self.colors['text_primary'],
                font=('SF Pro Display', 12, 'bold')).pack(pady=(15, 10))
        
        info_frame = tk.Frame(card, bg=self.colors['surface'])
        info_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=(0, 15))
        
        self.active_collision_name = tk.Label(info_frame, text="No active collision",
                                             bg=self.colors['surface'], fg=self.colors['text_secondary'],
                                             font=('SF Pro Display', 10))
        self.active_collision_name.pack(anchor=tk.W, pady=2)
        
        self.collision_progress_label = tk.Label(info_frame, text="Progress: 0%",
                                                bg=self.colors['surface'], fg=self.colors['primary'],
                                                font=('SF Pro Display', 10))
        self.collision_progress_label.pack(anchor=tk.W, pady=2)
        
        self.collision_eta_label = tk.Label(info_frame, text="ETA: ∞",
                                           bg=self.colors['surface'], fg=self.colors['accent'],
                                           font=('SF Pro Display', 10))
        self.collision_eta_label.pack(anchor=tk.W, pady=2)
        
        self.beam_energy_label = tk.Label(info_frame, text="Energy: 13 TeV",
                                         bg=self.colors['surface'], fg=self.colors['secondary'],
                                         font=('SF Pro Display', 10))
        self.beam_energy_label.pack(anchor=tk.W, pady=2)
    
    def create_beam_stats_card(self, parent, row, col):
        """Beam statistics card"""
        card = self.create_modern_card(parent, row, col)
        
        tk.Label(card, text="⚡ DAQ Jobs Status",
                bg=self.colors['surface'], fg=self.colors['text_primary'],
                font=('SF Pro Display', 12, 'bold')).pack(pady=(15, 10))
        
        beam_frame = tk.Frame(card, bg=self.colors['surface'])
        beam_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=(0, 15))
        
        self.beam_energy_display = tk.Label(beam_frame, text="13 TeV",
                                           bg=self.colors['surface'], fg=self.colors['secondary'],
                                           font=('SF Mono', 16, 'bold'))
        self.beam_energy_display.pack(pady=(0, 5))
        
        tk.Label(beam_frame, text="System Uptime",
                bg=self.colors['surface'], fg=self.colors['text_secondary'],
                font=('SF Pro Display', 9)).pack(pady=(0, 5))
        
        self.luminosity_label = tk.Label(beam_frame, text="Uptime: 00:00:00",
                                        bg=self.colors['surface'], fg=self.colors['text_secondary'],
                                        font=('SF Pro Display', 9))
        self.luminosity_label.pack(anchor=tk.W, pady=1)
        
        self.bunches_label = tk.Label(beam_frame, text="Bunches: 2556",
                                     bg=self.colors['surface'], fg=self.colors['text_secondary'],
                                     font=('SF Pro Display', 9))
        self.bunches_label.pack(anchor=tk.W, pady=1)
    
    def create_recent_events_card(self, parent, row, col):
        """Recent events card"""
        card = self.create_modern_card(parent, row, col)
        
        tk.Label(card, text="📡 Recent Events",
                bg=self.colors['surface'], fg=self.colors['text_primary'],
                font=('SF Pro Display', 12, 'bold')).pack(pady=(15, 10))
        
        events_frame = tk.Frame(card, bg=self.colors['surface'])
        events_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=(0, 15))
        
        self.recent_events_labels = []
        for i in range(5):
            label = tk.Label(events_frame, text="",
                           bg=self.colors['surface'], fg=self.colors['text_secondary'],
                           font=('SF Pro Display', 8))
            label.pack(anchor=tk.W, pady=1)
            self.recent_events_labels.append(label)
    
    def create_enhanced_jobs_table(self, parent):
        """Enhanced jobs table with modern styling"""
        # Card container
        table_card = tk.Frame(parent, bg=self.colors['surface'], relief='flat')
        table_card.pack(fill=tk.BOTH, expand=True)
        
        # Top accent
        accent_line = tk.Frame(table_card, bg=self.colors['primary'], height=2)
        accent_line.pack(fill=tk.X, side=tk.TOP)
        
        # Header
        header_frame = tk.Frame(table_card, bg=self.colors['surface'])
        header_frame.pack(fill=tk.X, padx=20, pady=(15, 10))
        
        tk.Label(header_frame, text="🗄️ Jobs Database",
                bg=self.colors['surface'], fg=self.colors['text_primary'],
                font=('SF Pro Display', 14, 'bold')).pack(side=tk.LEFT)
        
        # Refresh button with modern styling
        refresh_btn = tk.Button(header_frame, text="↻ Refresh",
                               command=self.refresh_jobs_display,
                               bg=self.colors['primary'], fg=self.colors['bg'],
                               font=('SF Pro Display', 10, 'bold'),
                               relief='flat', bd=0, padx=15, pady=5,
                               cursor='hand2')
        refresh_btn.pack(side=tk.RIGHT)
        
        # Table container with scrolling
        table_container = tk.Frame(table_card, bg=self.colors['surface'])
        table_container.pack(fill=tk.BOTH, expand=True, padx=20, pady=(0, 15))
        
        # Scrollbar
        scrollbar = ttk.Scrollbar(table_container)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Enhanced treeview
        self.jobs_tree = ttk.Treeview(table_container,
                                     style='Modern.Treeview',
                                     columns=('Status', 'Progress', 'Events', 'Errors', 'Duration', 'Start Time'),
                                     show='tree headings',
                                     yscrollcommand=scrollbar.set)
        
        # Configure columns with physics terminology
        self.jobs_tree.heading('#0', text='Job Name')
        self.jobs_tree.heading('Status', text='Status')
        self.jobs_tree.heading('Progress', text='Progress')
        self.jobs_tree.heading('Events', text='Events')
        self.jobs_tree.heading('Errors', text='Errors')
        self.jobs_tree.heading('Duration', text='Duration')
        self.jobs_tree.heading('Start Time', text='Start Time')
        
        self.jobs_tree.column('#0', width=200)
        self.jobs_tree.column('Status', width=120)
        self.jobs_tree.column('Progress', width=80)
        self.jobs_tree.column('Events', width=80)
        self.jobs_tree.column('Errors', width=80)
        self.jobs_tree.column('Duration', width=100)
        self.jobs_tree.column('Start Time', width=150)
        
        self.jobs_tree.pack(fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.jobs_tree.yview)
    
    def start_particle_animation(self):
        """Start subtle particle animation"""
        def animate():
            while self.monitoring:
                try:
                    # Animate header particle
                    new_particle = random.choice(self.particles)
                    self.root.after(0, lambda: self.particle_label.config(text=new_particle))
                    
                    # Rotate physics constants
                    const_name = random.choice(list(self.physics_constants.keys()))
                    const_value = self.physics_constants[const_name]
                    self.root.after(0, lambda: self.physics_label.config(text=f"{const_name} = {const_value}"))
                    
                    time.sleep(5)  # Change every 5 seconds
                except:
                    break
        
        animation_thread = threading.Thread(target=animate, daemon=True)
        animation_thread.start()
    
    def format_bytes(self, bytes_value):
        """Convert bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.1f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.1f} PB"
    
    def animate_progress_bar(self, bar, target_width):
        """Smooth progress bar animation"""
        try:
            if bar.winfo_exists():
                bar.place_configure(width=max(0, target_width))
        except:
            pass
    
    def draw_enhanced_graph(self, canvas, data, color, fill_color=None):
        """Draw enhanced graph with glow effects"""
        try:
            canvas.delete("all")
            
            if len(data) < 2:
                return
            
            width = canvas.winfo_width()
            height = canvas.winfo_height()
            
            if width <= 1 or height <= 1:
                return
            
            # Normalize data
            max_val = max(data) if max(data) > 0 else 100
            min_val = min(data)
            range_val = max_val - min_val if max_val > min_val else 1
            
            # Create smooth curve points
            points = []
            for i, value in enumerate(data):
                x = (i / (len(data) - 1)) * width
                y = height - ((value - min_val) / range_val) * height
                points.extend([x, y])
            
            # Draw with glow effect
            if len(points) >= 4:
                # Filled area
                if fill_color:
                    area_points = points + [width, height, 0, height]
                    canvas.create_polygon(area_points, fill=fill_color, outline="", stipple="gray12")
                
                # Glow layers
                for i in range(3):
                    canvas.create_line(points, fill=color, width=3-i, smooth=True, stipple='gray75')
                
                # Main line
                canvas.create_line(points, fill=color, width=2, smooth=True)
                
        except Exception as e:
            print(f"Error drawing enhanced graph: {e}")
    
    def process_job_update(self, message):
        """Process job update with enhanced tracking"""
        try:
            job_name = message.get('name', 'Unknown Collision')
            status = message.get('status', 'Unknown')
            progress = message.get('progress', 0)
            details = message.get('details', {})
            timestamp = message.get('timestamp', time.time())
            
            # Create job ID for new jobs
            if status in ['Starting', 'Initializing'] or self.current_job_id is None:
                self.current_job_id = f"{job_name}_{int(timestamp)}"
                self.current_job_data = {
                    'name': job_name,
                    'id': self.current_job_id,
                    'start_time': timestamp,
                    'end_time': None,
                    'status': status,
                    'progress': progress,
                    'details': details,
                    'completed': details.get('Completed', 0),
                    'failed': details.get('Failed', 0),
                    'total': details.get('Total', details.get('Total Items', 0)),
                    'updates': [],
                    'resource_snapshots': []
                }
                self.jobs_history[self.current_job_id] = self.current_job_data
                
                # Capture initial resource state
                self.job_start_resources = {
                    'cpu': psutil.cpu_percent(),
                    'memory': psutil.virtual_memory().percent,
                    'timestamp': timestamp
                }
            
            # Update current job
            if self.current_job_id and self.current_job_id in self.jobs_history:
                job_data = self.jobs_history[self.current_job_id]
                job_data.update({
                    'status': status,
                    'progress': progress,
                    'details': details,
                    'completed': details.get('Completed', job_data.get('completed', 0)),
                    'failed': details.get('Failed', job_data.get('failed', 0)),
                    'total': details.get('Total', details.get('Total Items', job_data.get('total', 0)))
                })
                
                # Add resource snapshot
                current_cpu = psutil.cpu_percent()
                current_memory = psutil.virtual_memory().percent
                
                job_data['resource_snapshots'].append({
                    'timestamp': timestamp,
                    'cpu': current_cpu,
                    'memory': current_memory,
                    'progress': progress
                })
                
                # Update job-specific resource histories
                self.job_cpu_history.append(current_cpu)
                self.job_ram_history.append(current_memory)
                self.job_progress_history.append(progress)
                
                # Add update to history
                job_data['updates'].append({
                    'timestamp': timestamp,
                    'status': status,
                    'progress': progress,
                    'details': details
                })
                
                # If job is finished
                if status in ['Completed', 'Completed with Errors', 'Failed']:
                    job_data['end_time'] = timestamp
                    self.current_job_id = None
                    # Clear job-specific histories
                    self.job_cpu_history.clear()
                    self.job_ram_history.clear()
                    self.job_progress_history.clear()
                
                self.save_jobs_history()
                
        except Exception as e:
            print(f"Error processing collision data: {e}")
    
    def update_system_metrics(self):
        """Update all system metrics"""
        try:
            current_time = time.time()
            time_delta = current_time - self.last_time
            
            # Update time with physics flair
            current_dt = datetime.now()
            self.time_label.config(text=current_dt.strftime("%H:%M:%S.%f")[:-3])
            
            # CPU
            cpu_percent = psutil.cpu_percent(interval=None)
            self.cpu_history.append(cpu_percent)
            
            self.cpu_metric.config(text=f"{cpu_percent:.1f}%")
            
            cpu_bar_width = (cpu_percent / 100) * self.cpu_bg_bar.winfo_width()
            self.animate_progress_bar(self.cpu_progress_bar, cpu_bar_width)
            
            # Dynamic color based on load
            if cpu_percent > 80:
                color = self.colors['error']
            elif cpu_percent > 60:
                color = self.colors['warning']
            else:
                color = self.colors['primary']
            self.cpu_progress_bar.config(bg=color)
            self.cpu_metric.config(fg=color)
            
            # CPU details
            cpu_count = psutil.cpu_count()
            try:
                cpu_freq = psutil.cpu_freq()
                freq_ghz = cpu_freq.current / 1000 if cpu_freq else 0
                freq_text = f"Frequency: {freq_ghz:.2f} GHz"
            except:
                freq_text = "Frequency: Unknown"
            
            self.cpu_cores_label.config(text=f"Quantum Cores: {cpu_count}")
            self.cpu_freq_label.config(text=freq_text)
            
            # Memory
            memory = psutil.virtual_memory()
            self.ram_history.append(memory.percent)
            
            self.memory_metric.config(text=f"{memory.percent:.1f}%")
            
            memory_bar_width = (memory.percent / 100) * self.memory_bg_bar.winfo_width()
            self.animate_progress_bar(self.memory_progress_bar, memory_bar_width)
            
            # Memory color coding
            if memory.percent > 85:
                mem_color = self.colors['error']
            elif memory.percent > 70:
                mem_color = self.colors['warning']
            else:
                mem_color = self.colors['accent']
            self.memory_progress_bar.config(bg=mem_color)
            self.memory_metric.config(fg=mem_color)
            
            self.memory_used_label.config(text=f"Allocated: {self.format_bytes(memory.used)}")
            self.memory_available_label.config(text=f"Available: {self.format_bytes(memory.available)}")
            
            # Network
            current_net_io = psutil.net_io_counters()
            
            if time_delta > 0:
                bytes_sent = current_net_io.bytes_sent - self.last_net_io.bytes_sent
                bytes_recv = current_net_io.bytes_recv - self.last_net_io.bytes_recv
                
                upload_speed = bytes_sent / time_delta
                download_speed = bytes_recv / time_delta
                total_speed = upload_speed + download_speed
                
                self.net_history.append(total_speed / 1024)
                
                self.network_metric.config(text=f"{self.format_bytes(total_speed)}/s")
                self.upload_label.config(text=f"↑ {self.format_bytes(upload_speed)}/s")
                self.download_label.config(text=f"↓ {self.format_bytes(download_speed)}/s")
            
            # Storage
            home_usage = psutil.disk_usage(os.path.expanduser("~"))
            storage_percent = (home_usage.used / home_usage.total) * 100
            
            self.storage_metric.config(text=f"{storage_percent:.1f}%")
            
            storage_bar_width = (storage_percent / 100) * self.storage_bg_bar.winfo_width()
            self.animate_progress_bar(self.storage_progress_bar, storage_bar_width)
            
            # Storage color
            if storage_percent > 90:
                storage_color = self.colors['error']
            elif storage_percent > 75:
                storage_color = self.colors['warning']
            else:
                storage_color = self.colors['warning']
            self.storage_progress_bar.config(bg=storage_color)
            self.storage_metric.config(fg=storage_color)
            
            self.storage_used_label.config(text=f"Occupied: {self.format_bytes(home_usage.used)}")
            self.storage_free_label.config(text=f"Available: {self.format_bytes(home_usage.free)}")
            
            # Processes
            processes = []
            for proc in psutil.process_iter(['pid', 'name', 'cpu_percent']):
                try:
                    proc_info = proc.info
                    if proc_info['cpu_percent'] is not None and proc_info['cpu_percent'] > 0:
                        processes.append(proc_info)
                except:
                    continue
            
            processes.sort(key=lambda x: x['cpu_percent'], reverse=True)
            total_processes = len(list(psutil.process_iter()))
            
            self.process_count_metric.config(text=str(total_processes))
            
            for i, label in enumerate(self.process_labels):
                if i < len(processes):
                    proc = processes[i]
                    name = proc['name'][:14] + "…" if len(proc['name']) > 14 else proc['name']
                    label.config(text=f"{name:<16} {proc['cpu_percent']:>4.1f}%")
                else:
                    label.config(text="")
            
            # System info
            boot_time = psutil.boot_time()
            uptime_seconds = time.time() - boot_time
            hours = int(uptime_seconds // 3600)
            minutes = int((uptime_seconds % 3600) // 60)
            seconds = int(uptime_seconds % 60)
            self.uptime_label.config(text=f"{hours:02d}:{minutes:02d}:{seconds:02d}")
            
            # Hostname
            try:
                self.hostname_label.config(text=f"Node: {socket.gethostname()}")
            except:
                pass
            
            # Temperature with physics units
            try:
                temps = psutil.sensors_temperatures()
                if temps:
                    temp_info = list(temps.values())[0][0]
                    temp_k = temp_info.current + 273.15  # Convert to Kelvin
                    self.temperature_label.config(text=f"Core Temp: {temp_info.current:.1f}°C ({temp_k:.1f}K)")
                else:
                    self.temperature_label.config(text="Core Temp: N/A")
            except:
                self.temperature_label.config(text="Core Temp: N/A")
            
            # Load average
            try:
                load_avg = os.getloadavg()
                self.load_label.config(text=f"Load Vector: {load_avg[0]:.2f}")
            except:
                self.load_label.config(text="Load Vector: N/A")
            
            # Update graphs with glow effects
            self.draw_enhanced_graph(self.cpu_graph, self.cpu_history, 
                                   self.colors['primary'], self.colors['glow'])
            self.draw_enhanced_graph(self.memory_graph, self.ram_history, 
                                   self.colors['accent'], self.colors['glow'])
            self.draw_enhanced_graph(self.network_graph, self.net_history, 
                                   self.colors['secondary'], self.colors['glow'])
            
            # Update job resource graphs if job is active
            if self.current_job_id:
                self.draw_enhanced_graph(self.job_cpu_graph, self.job_cpu_history, 
                                       self.colors['primary'], self.colors['glow'])
                self.draw_enhanced_graph(self.job_mem_graph, self.job_ram_history, 
                                       self.colors['accent'], self.colors['glow'])
                self.draw_enhanced_graph(self.job_progress_graph, self.job_progress_history, 
                                       self.colors['secondary'], self.colors['glow'])
            
            # Update counters
            self.last_net_io = current_net_io
            self.last_time = current_time
            
        except Exception as e:
            print(f"Error updating quantum metrics: {e}")
    
    def update_external_process_display(self):
        """Update enhanced job display"""
        try:
            # Check for new messages
            while not self.process_queue.empty():
                try:
                    message = self.process_queue.get_nowait()
                    self.external_process_data.update(message)
                    self.process_job_update(message)
                except queue.Empty:
                    break
            
            # Update display
            data = self.external_process_data
            
            self.job_name_label.config(text=data['name'])
            self.job_status_label.config(text=data['status'])
            
            # Update progress with precision
            progress = data['progress']
            self.job_progress_label.config(text=f"{progress:.2f}%")
            
            progress_width = (progress / 100) * self.job_progress_bg.winfo_width()
            self.animate_progress_bar(self.job_progress_bar, progress_width)
            
            # Update collision dot
            if data['status'].lower() in ['processing', 'running']:
                dot_color = self.colors['success']
            elif data['status'].lower() in ['error', 'failed']:
                dot_color = self.colors['error']
            else:
                dot_color = self.colors['border']
            
            self.collision_dot.delete("all")
            self.collision_dot.create_oval(1, 1, 11, 11, fill=dot_color, outline="")
            
            # Update job metrics with physics terminology
            details = data.get('details', {})
            completed = details.get('Completed', 0)
            failed = details.get('Failed', 0)
            eta = details.get('ETA', '∞')
            
            self.job_completed_label.config(text=f"Events: {completed}")
            self.job_failed_label.config(text=f"Errors: {failed}")
            self.job_eta_label.config(text=f"ETA: {eta}")
            
            # Update jobs tab displays
            self.update_jobs_displays()
            
        except Exception as e:
            print(f"Error updating collision display: {e}")
    
    def update_jobs_displays(self):
        """Update jobs tab displays"""
        try:
            # Calculate collision statistics
            total_collisions = len(self.jobs_history)
            successful_collisions = sum(1 for job in self.jobs_history.values() 
                                       if job.get('status', '').startswith('Completed'))
            failed_collisions = sum(1 for job in self.jobs_history.values() 
                                  if job.get('status', '') == 'Failed')
            efficiency = (successful_collisions / total_collisions * 100) if total_collisions > 0 else 0
            
            # Update stats
            self.total_collisions_label.config(text=f"Total Runs: {total_collisions}")
            self.successful_collisions_label.config(text=f"Successful: {successful_collisions}")
            self.failed_collisions_label.config(text=f"Failed: {failed_collisions}")
            self.efficiency_label.config(text=f"Efficiency: {efficiency:.1f}%")
            
            # Update active collision
            if self.current_job_id and self.current_job_id in self.jobs_history:
                job_data = self.jobs_history[self.current_job_id]
                self.active_collision_name.config(text=job_data.get('name', 'Unknown'))
                self.collision_progress_label.config(text=f"Progress: {job_data.get('progress', 0):.1f}%")
                
                eta = job_data.get('details', {}).get('ETA', '∞')
                self.collision_eta_label.config(text=f"ETA: {eta}")
                
                # Random beam energy from CERN ranges
                energy = random.choice(self.lhc_energies)
                self.beam_energy_label.config(text=f"Energy: {energy}")
            else:
                self.active_collision_name.config(text="No active collision")
                self.collision_progress_label.config(text="Progress: 0%")
                self.collision_eta_label.config(text="ETA: ∞")
            
            # Update recent events
            recent_jobs = sorted(self.jobs_history.values(), 
                               key=lambda x: x.get('start_time', 0), reverse=True)[:5]
            
            for i, label in enumerate(self.recent_events_labels):
                if i < len(recent_jobs):
                    job = recent_jobs[i]
                    start_time = datetime.fromtimestamp(job.get('start_time', 0)).strftime("%H:%M")
                    status = job.get('status', 'Unknown')
                    particle = random.choice(['p', 'Pb', 'p+', 'e⁻'])  # Physics particles
                    label.config(text=f"{start_time} {particle} collision • {status}")
                else:
                    label.config(text="")
            
        except Exception as e:
            print(f"Error updating collision displays: {e}")
    
    def refresh_jobs_display(self):
        """Refresh enhanced jobs table"""
        try:
            # Clear existing items
            for item in self.jobs_tree.get_children():
                self.jobs_tree.delete(item)
            
            # Add jobs with enhanced display
            for job_id, job_data in sorted(self.jobs_history.items(), 
                                         key=lambda x: x[1].get('start_time', 0), reverse=True):
                
                start_time = datetime.fromtimestamp(job_data.get('start_time', 0)).strftime("%Y-%m-%d %H:%M:%S")
                duration = "Ongoing"
                
                if job_data.get('end_time'):
                    duration_seconds = job_data['end_time'] - job_data.get('start_time', 0)
                    hours = int(duration_seconds // 3600)
                    minutes = int((duration_seconds % 3600) // 60)
                    seconds = int(duration_seconds % 60)
                    duration = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                
                # Add physics flair to job names
                name = job_data.get('name', 'Unknown')
                if 'FT0' in name:
                    name = f"⚡ {name}"
                elif 'FV0' in name:
                    name = f"🔬 {name}"
                
                self.jobs_tree.insert('', 'end',
                                     text=name,
                                     values=(
                                         job_data.get('status', 'Unknown'),
                                         f"{job_data.get('progress', 0):.1f}%",
                                         job_data.get('completed', 0),
                                         job_data.get('failed', 0),
                                         duration,
                                         start_time
                                     ))
        except Exception as e:
            print(f"Error refreshing collision database: {e}")
    
    def start_process_listener(self):
        """Start listening for collision data"""
        def listener():
            try:
                server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server_socket.bind(('localhost', 9999))
                server_socket.listen(1)
                
                print("CERN Monitor listening for collision data on port 9999...")
                
                while self.monitoring:
                    try:
                        server_socket.settimeout(1.0)
                        client_socket, addr = server_socket.accept()
                        
                        data = client_socket.recv(1024).decode('utf-8')
                        if data:
                            try:
                                message = json.loads(data)
                                self.process_queue.put(message)
                            except json.JSONDecodeError:
                                print(f"Invalid collision data received: {data}")
                        
                        client_socket.close()
                        
                    except socket.timeout:
                        continue
                    except Exception as e:
                        print(f"Error in collision listener: {e}")
                        
                server_socket.close()
                
            except Exception as e:
                print(f"Failed to start collision listener: {e}")
        
        listener_thread = threading.Thread(target=listener, daemon=True)
        listener_thread.start()
    
    def monitor_loop(self):
        """Main monitoring loop"""
        while self.monitoring:
            try:
                self.root.after(0, self.update_system_metrics)
                self.root.after(0, self.update_external_process_display)
                time.sleep(1)
            except Exception as e:
                print(f"Error in quantum monitoring loop: {e}")
                break
    
    def start_monitoring(self):
        """Start quantum monitoring"""
        if self.monitoring:
            monitor_thread = threading.Thread(target=self.monitor_loop, daemon=True)
            monitor_thread.start()
            
            # Initial refresh
            self.root.after(1000, self.refresh_jobs_display)
    
    def on_closing(self):
        """Handle application closing"""
        self.monitoring = False
        self.save_jobs_history()
        self.root.quit()
        self.root.destroy()

def main():
    """Main application entry point"""
    try:
        import psutil
    except ImportError:
        print("ERROR: psutil library required for quantum monitoring")
        print("Install with: pip3 install psutil")
        return
    
    root = tk.Tk()
    app = ModernCERNMonitor(root)
    
    try:
        root.mainloop()
    except KeyboardInterrupt:
        print("\nBeam shutdown initiated...")
        app.on_closing()

if __name__ == "__main__":
    main()