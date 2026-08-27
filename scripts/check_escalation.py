"""Quick diagnostic: which AUTH_PATTERN matches scrapingcourse.com homepage HTML."""
import re
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import httpx

resp = httpx.get("https://www.scrapingcourse.com/", follow_redirects=True)
html = resp.text
print(f"HTML length: {len(html)}")

# Import the actual patterns from the codebase
from app.common.constants.worker_assignment import AUTH_PATTERNS

for i, pat in enumerate(AUTH_PATTERNS):
    compiled = re.compile(pat, re.IGNORECASE)
    m = compiled.search(html)
    if m:
        start = max(0, m.start() - 60)
        end = min(len(html), m.end() + 60)
        print(f"\n  MATCH #{i}: {pat}")
        print(f"  Context: ...{html[start:end]}...")

# Also check the old patterns
OLD_PATTERNS = [
    r'<input[^>]*type\s*=\s*["\']?password["\']?',
    r'\b(?:create\s+account|register\s+now|sign\s+up|registration|required\s+registration)\b',
    r'cf-chl-|challenge-platform|turnstile|hcaptcha\.com|recaptcha/api\.js',
    r'perimeterx|px-captcha|_px3',
    r'datadome',
    r'akamai.*bot|bm-verify',
    r'incapsula|imperva',
]
print("\n\n--- Old patterns (potential false positives) ---")
for i, pat in enumerate(OLD_PATTERNS):
    compiled = re.compile(pat, re.IGNORECASE)
    m = compiled.search(html)
    if m:
        start = max(0, m.start() - 60)
        end = min(len(html), m.end() + 60)
        print(f"\n  OLD MATCH #{i}: {pat}")
        print(f"  Context: ...{html[start:end]}...")
