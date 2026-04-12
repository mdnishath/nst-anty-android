import requests
import re

def get_review_url(gmb_share_url):
    """Convert a GMB share URL to a direct Write Review URL."""
    try:
        # Step 1: Resolve short URL to full Google Maps URL
        response = requests.head(gmb_share_url, allow_redirects=True, timeout=10)
        resolved_url = response.url
        print(f"Resolved URL: {resolved_url}\n")

        # Step 2: Extract HEX CID from !1s parameter
        match = re.search(r'!1s(0x[0-9a-fA-F]+:0x[0-9a-fA-F]+)', resolved_url)
        if not match:
            print("ERROR: HEX CID not found in URL")
            return None

        hex_cid = match.group(1)
        print(f"HEX CID: {hex_cid}\n")

        # Step 3: Build Write Review URL
        review_url = f"https://www.google.com/maps/place//data=!4m3!3m2!1s{hex_cid}!12e1"
        return review_url

    except Exception as e:
        print(f"ERROR: {e}")
        return None


# ============ TEST ============
gmb_url = "https://maps.app.goo.gl/yvBRbChDD1dDjwUe6"

print("=" * 60)
print("GMB Share URL → Write Review Link Generator")
print("=" * 60)
print(f"\nInput URL: {gmb_url}\n")

review_link = get_review_url(gmb_url)

if review_link:
    print(f"Write Review URL:\n{review_link}")
else:
    print("Failed to generate review link")
