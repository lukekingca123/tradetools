"""
YC Startup Analyzer - A tool for analyzing Y Combinator startups
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import List, Dict
import time
from datetime import datetime
import logging
import json

class YCAnalyzer:
    """YC Startup data analyzer and collector"""
    
    def __init__(self):
        self.base_url = "https://www.ycombinator.com"
        self.companies_url = "https://www.ycombinator.com/companies"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.logger = logging.getLogger(__name__)

    def fetch_companies(self, batch: str = None) -> List[Dict]:
        """
        Fetch YC companies data
        Args:
            batch: Optional YC batch (e.g., 'W23', 'S22')
        Returns:
            List of company data dictionaries
        """
        try:
            # YC now uses a GraphQL API endpoint
            api_url = f"{self.base_url}/api/companies"
            params = {}
            if batch:
                params['batch'] = batch
            
            response = requests.get(api_url, headers=self.headers, params=params)
            response.raise_for_status()
            
            return response.json()['companies']
        except Exception as e:
            self.logger.error(f"Error fetching companies: {str(e)}")
            return []

    def analyze_trends(self, companies: List[Dict]) -> Dict:
        """
        Analyze trends in YC companies
        Args:
            companies: List of company data
        Returns:
            Dictionary containing trend analysis
        """
        df = pd.DataFrame(companies)
        
        analysis = {
            'total_companies': len(df),
            'industries': df['industry'].value_counts().to_dict(),
            'locations': df['location'].value_counts().to_dict(),
            'batch_distribution': df['batch'].value_counts().to_dict(),
            'funding_ranges': df['total_funding'].describe().to_dict() if 'total_funding' in df.columns else None,
        }
        
        return analysis

    def get_company_details(self, company_id: str) -> Dict:
        """
        Get detailed information about a specific company
        Args:
            company_id: Company identifier
        Returns:
            Dictionary containing company details
        """
        try:
            api_url = f"{self.base_url}/api/company/{company_id}"
            response = requests.get(api_url, headers=self.headers)
            response.raise_for_status()
            
            return response.json()
        except Exception as e:
            self.logger.error(f"Error fetching company details: {str(e)}")
            return {}

    def export_to_csv(self, data: List[Dict], filename: str):
        """
        Export company data to CSV
        Args:
            data: List of company data dictionaries
            filename: Output filename
        """
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        self.logger.info(f"Data exported to {filename}")

    def get_investment_insights(self, companies: List[Dict]) -> Dict:
        """
        Generate investment insights from company data
        Args:
            companies: List of company data
        Returns:
            Dictionary containing investment insights
        """
        df = pd.DataFrame(companies)
        
        insights = {
            'fast_growing_sectors': df.groupby('industry').size().sort_values(ascending=False).head(10).to_dict(),
            'successful_business_models': df['business_model'].value_counts().head(5).to_dict() if 'business_model' in df.columns else None,
            'geographic_hotspots': df['location'].value_counts().head(10).to_dict(),
            'recent_trends': df[df['batch'] == df['batch'].max()]['industry'].value_counts().head(5).to_dict(),
        }
        
        return insights

def main():
    analyzer = YCAnalyzer()
    
    # Fetch recent companies
    companies = analyzer.fetch_companies()
    
    # Analyze trends
    trends = analyzer.analyze_trends(companies)
    
    # Get investment insights
    insights = analyzer.get_investment_insights(companies)
    
    # Export data
    timestamp = datetime.now().strftime('%Y%m%d')
    analyzer.export_to_csv(companies, f'yc_companies_{timestamp}.csv')
    
    # Save analysis results
    with open(f'yc_analysis_{timestamp}.json', 'w') as f:
        json.dump({
            'trends': trends,
            'insights': insights
        }, f, indent=4)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
