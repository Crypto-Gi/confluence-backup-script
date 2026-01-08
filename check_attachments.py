#!/usr/bin/env python
from src.client import ConfluenceClient
from src.utils import load_app_config

config = load_app_config('.env', 'config.yaml')

dest = ConfluenceClient(
    base_url=config.destination.base_url,
    user_email=config.destination.user_email,
    api_token=config.destination.api_token,
    read_only=True
)

# Check a page that shows "Unchanged" attachments
pages = list(dest.list_pages_in_space('17301506', limit=200))
page = next((p for p in pages if 'UsingiPerfToVerifyWANPerformance' in p.title), None)

if page:
    print('Page:', page.title, 'ID:', page.id)
    attachments = list(dest.list_page_attachments(page.id))
    print('Attachments in destination:', len(attachments))
    for att in attachments[:5]:
        print('  -', att.title, '(' + str(att.file_size) + ' bytes)')
        
# Also check SRX Basics 101
srx_page = next((p for p in pages if 'SRX Basics 101' in p.title), None)
if srx_page:
    print('\nSRX Basics 101 ID:', srx_page.id)
    srx_atts = list(dest.list_page_attachments(srx_page.id))
    print('Attachments:', len(srx_atts))
