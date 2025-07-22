#!/usr/bin/env python3
"""
monitor_client.py - Communication helper for sending updates to the system dashboard
Add this to your data_fetch.py script to enable real-time monitoring
"""

import socket
import json
import time
from typing import Dict, Any, Optional

class DashboardClient:
    def __init__(self, host='localhost', port=9999):
        self.host = host
        self.port = port
        self.process_name = "Data Processing Job"
        self.start_time = time.time()
        
    def send_update(self, status: str, progress: float = 0, details: Optional[Dict[str, Any]] = None):
        """Send status update to dashboard"""
        try:
            message = {
                'name': self.process_name,
                'status': status,
                'progress': progress,
                'details': details or {},
                'timestamp': time.time()
            }
            
            # Send via socket
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.settimeout(2.0)
            client_socket.connect((self.host, self.port))
            client_socket.send(json.dumps(message).encode('utf-8'))
            client_socket.close()
            
        except Exception as e:
            # Silently fail if dashboard is not running
            pass
    
    def set_process_name(self, name: str):
        """Set the process name displayed in dashboard"""
        self.process_name = name
    
    def start_processing(self, total_items: int, detector: str = ""):
        """Signal start of processing"""
        details = {
            'Total Items': total_items,
            'Start Time': time.strftime('%H:%M:%S'),
            'Detector': detector
        }
        
        if detector:
            details['Detector'] = detector
            
        self.send_update("Starting", 0, details)
    
    def update_progress(self, completed: int, total: int, current_item: str = "", 
                       failed: int = 0, detector: str = ""):
        """Update processing progress"""
        progress = ((completed + failed) / total * 100) if total > 0 else 0
        elapsed = time.time() - self.start_time
        
        details = {
            'Completed': completed,
            'Failed': failed,
            'Total': total,
            'Remaining': total - completed - failed,
            'Current Run': current_item,
            'Elapsed Time': f"{elapsed:.0f}s"
        }
        
        if detector:
            details['Detector'] = detector
        
        if (completed + failed) > 0:
            eta = (elapsed / (completed + failed)) * (total - completed - failed)
            details['ETA'] = f"{eta:.0f}s"
            details['Success Rate'] = f"{(completed/(completed+failed)*100):.1f}%"
        
        status = "Processing" if (completed + failed) < total else "Finishing"
        self.send_update(status, progress, details)
    
    def report_success(self, item: str):
        """Report successful completion of an item"""
        self.send_update("Processing", details={'Last Success': item})
    
    def report_error(self, item: str, error: str):
        """Report error for an item"""
        self.send_update("Processing", details={'Last Error': f"{item}: {error}"})
    
    def finish_processing(self, completed: int, failed: int, detector: str = ""):
        """Signal end of processing"""
        total_time = time.time() - self.start_time
        details = {
            'Total Completed': completed,
            'Total Failed': failed,
            'Total Time': f"{total_time:.0f}s",
            'Final Success Rate': f"{(completed/(completed+failed)*100):.1f}%" if (completed+failed) > 0 else "0%",
            'End Time': time.strftime('%H:%M:%S')
        }
        
        if detector:
            details['Detector'] = detector
        
        status = "Completed" if failed == 0 else "Completed with Errors"
        self.send_update(status, 100, details)