#!/usr/bin/env python3
"""
ALICE FT0 HV Corrections Fetcher

This script fetches High Voltage (HV) correction events for the FT0 detector
from the ALICE Bookkeeping API and saves them in JSON format.
"""

import os
import json
import re
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging
from dotenv import load_dotenv
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FT0HVFetcher:
    """Fetches and processes FT0 HV correction events from ALICE Bookkeeping API."""
    
    def __init__(self):
        self.base_url = "https://ali-bookkeeping.cern.ch/api/logs"
        self.token = os.getenv('ALICE_BK_TOKEN')
        
        if not self.token:
            raise ValueError("ALICE_BK_TOKEN environment variable is required")
    
    def build_api_url(self, offset: int = 0, limit: int = 50) -> str:
        """Build the API URL with filters for FT0 HV logs."""
        params = {
            'filter[tags][values]': 'FT0',
            'filter[tags][operation]': 'and',
            'page[offset]': offset,
            'page[limit]': limit,
            'token': self.token
        }
        
        param_string = '&'.join([f"{k}={v}" for k, v in params.items()])
        return f"{self.base_url}?{param_string}"
    
    def fetch_logs_page(self, offset: int = 0, limit: int = 50) -> Optional[Dict[str, Any]]:
        """Fetch a single page of logs from the API."""
        url = self.build_api_url(offset, limit)
        
        try:
            logger.info(f"Fetching logs from offset {offset}")
            response = requests.get(url, timeout=30, verify=False)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from API: {e}")
            return None
    
    def is_hv_config_update(self, title: str) -> bool:
        """
        Check if the log title indicates an HV configuration update.
        
        Looks for titles containing FT0, HV, and magnetic field indicators.
        """
        title_lower = title.lower()
        
        # Must contain FT0 and HV
        if 'ft0' not in title_lower or 'hv' not in title_lower:
            return False
        
        # Look for magnetic field indicators and config update keywords
        field_patterns = [
            r'[+-]?\d+\.?\d*t',  # Matches patterns like +0.5T, -0.5T, 0.0T
            r'without\s+b-?field',
            r'no\s+b-?field'
        ]
        
        config_keywords = ['config', 'updated', 'fine-tuning', 'default']
        
        has_field_info = any(re.search(pattern, title_lower) for pattern in field_patterns)
        has_config_keyword = any(keyword in title_lower for keyword in config_keywords)
        
        return has_field_info and has_config_keyword
    
    def extract_magnetic_field(self, title: str) -> Optional[str]:
        """Extract magnetic field value from the title."""
        # Look for patterns like +0.5T, -0.5T, 0.0T
        field_match = re.search(r'([+-]?\d+\.?\d*)t', title.lower())
        if field_match:
            return f"{field_match.group(1)}T"
        
        # Look for "without B-field" or similar
        if re.search(r'without\s+b-?field|no\s+b-?field', title.lower()):
            return "0.0T"
        
        return None
    
    def process_log_entry(self, log_entry: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single log entry and extract relevant information."""
        timestamp_ms = log_entry.get('createdAt', 0)
        timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000.0)
        
        author_info = log_entry.get('author', {})
        magnetic_field = self.extract_magnetic_field(log_entry.get('title', ''))
        
        return {
            'id': log_entry.get('id'),
            'title': log_entry.get('title'),
            'timestamp': timestamp_dt.isoformat(),
            'timestamp_unix': timestamp_ms,
            'magnetic_field': magnetic_field,
            'author': {
                'name': author_info.get('name'),
                'id': author_info.get('id'),
                'external_id': author_info.get('externalId')
            },
            'text_preview': log_entry.get('text', '')[:200] + '...' if len(log_entry.get('text', '')) > 200 else log_entry.get('text', '')
        }
    
    def fetch_all_hv_corrections(self) -> List[Dict[str, Any]]:
        """Fetch all HV correction events from the API."""
        all_events = []
        offset = 0
        limit = 50
        
        while True:
            response_data = self.fetch_logs_page(offset, limit)
            
            if not response_data:
                logger.error("Failed to fetch data, stopping")
                break
            
            logs = response_data.get('data', [])
            if not logs:
                logger.info("No more logs to fetch")
                break
            
            # Filter and process relevant logs
            for log_entry in logs:
                title = log_entry.get('title', '')
                if self.is_hv_config_update(title):
                    processed_entry = self.process_log_entry(log_entry)
                    all_events.append(processed_entry)
                    logger.info(f"Found HV correction: {title}")
            
            # Check if we've reached the end
            meta = response_data.get('meta', {})
            page_info = meta.get('page', {})
            total_count = page_info.get('totalCount', 0)
            
            offset += limit
            if offset >= total_count:
                logger.info(f"Reached end of data (total: {total_count})")
                break
        
        return all_events
    
    def save_to_json(self, events: List[Dict[str, Any]], filename: str = 'ft0_hv_corrections.json'):
        """Save the events to a JSON file."""
        output_data = {
            'metadata': {
                'total_events': len(events),
                'generated_at': datetime.now().isoformat(),
                'description': 'FT0 HV configuration update events from ALICE Bookkeeping'
            },
            'events': sorted(events, key=lambda x: x['timestamp_unix'], reverse=True)
        }
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(events)} events to {filename}")
        except IOError as e:
            logger.error(f"Error saving to file: {e}")


def main():
    """Main execution function."""
    try:
        fetcher = FT0HVFetcher()
        
        logger.info("Starting FT0 HV corrections fetch...")
        events = fetcher.fetch_all_hv_corrections()
        
        if events:
            fetcher.save_to_json(events)
            
            # Print summary
            print(f"\n📊 Summary:")
            print(f"Found {len(events)} HV correction events")
            
            # Group by magnetic field
            field_counts = {}
            for event in events:
                field = event.get('magnetic_field', 'Unknown')
                field_counts[field] = field_counts.get(field, 0) + 1
            
            print(f"\nBreakdown by magnetic field:")
            for field, count in sorted(field_counts.items()):
                print(f"  {field}: {count} events")
            
            print(f"\nMost recent events:")
            for event in sorted(events, key=lambda x: x['timestamp_unix'], reverse=True)[:3]:
                print(f"  {event['timestamp'][:19]} - {event['title']}")
                
        else:
            logger.warning("No HV correction events found")
            
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        raise


if __name__ == "__main__":
    main()