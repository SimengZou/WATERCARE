import warnings
warnings.filterwarnings('ignore', category=Warning, message='.*urllib3.*')

import argparse
import os
import re

from dotenv import load_dotenv

from scripts.util.infor_api import InforAPIClient
from scripts.util.manager import Manager

def main():
    parser = argparse.ArgumentParser(description="DBT Code Generation Pipeline")
    parser.add_argument('--json-path', type=str, default='./tmp/json', 
                        help="Path to json schemas directory (default: './tmp/json')")
    parser.add_argument('--model-path', type=str, default='./models/curated', 
                        help="Path to output dbt models directory (default: './models/curated')")
    parser.add_argument('--download', type=str, default='^(IPS|DEPM|EAM|LN)_',
                        help="Target systems to download via regex matching (default: '^(IPS|DEPM|EAM|LN)_').")
    parser.add_argument('--no-generate', action='store_false', dest='generate_models', 
                        help="Disable generation of dbt models (default: enabled)")
    parser.add_argument('--no-sources', action='store_false', dest='generate_sources', 
                        help="Disable updating __sources.yml definitions (default: enabled)")
    parser.add_argument('--batch-size', type=int, default=250, 
                        help="Number of models per stage tag for parallel execution (default: 100)")
    parser.add_argument('--no-docs', action='store_false', dest='generate_docs', 
                        help="Disable generation of markdown documentation (default: enabled)")
    parser.add_argument('--docs-path', type=str, default='./docs',
                        help="Output directory for generated docs (default: './docs')")
    
    args = parser.parse_args()

    load_dotenv()

    api_client = InforAPIClient(
        environment=os.environ.get('INFOR_API_ENVIRONMENT', ''),
        auth_url=os.environ.get('INFOR_API_AUTH_URL', ''),
        data_url=os.environ.get('INFOR_API_DATA_URL', ''),
        client_id=os.environ.get('INFOR_API_CLIENT_ID', ''),
        client_secret=os.environ.get('INFOR_API_CLIENT_SECRET', ''),
        username=os.environ.get('INFOR_API_USERNAME', ''),
        password=os.environ.get('INFOR_API_PASSWORD', '')
    )

    manager = Manager(args.json_path, args.model_path, api_client)
    
    if args.download:
        regex_pattern = args.download
        manager.download_api_schema(re.compile(regex_pattern, flags=re.IGNORECASE))
        
    if args.generate_models:
        manager.generate_dbt_models(generate_sources=args.generate_sources, batch_size=args.batch_size)

    if args.generate_docs:
        manager.generate_docs(output_dir=args.docs_path)

if __name__ == "__main__":
    main()
