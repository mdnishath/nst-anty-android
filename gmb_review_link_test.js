const https = require('https');
const http = require('http');
const url = require('url');

function resolveRedirects(inputUrl, maxRedirects = 10) {
    return new Promise((resolve, reject) => {
        let redirectCount = 0;

        function follow(currentUrl) {
            if (redirectCount >= maxRedirects) {
                return reject(new Error('Too many redirects'));
            }

            const parsed = new URL(currentUrl);
            const client = parsed.protocol === 'https:' ? https : http;

            const req = client.request(currentUrl, { method: 'HEAD' }, (res) => {
                if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
                    redirectCount++;
                    let nextUrl = res.headers.location;
                    // Handle relative URLs
                    if (!nextUrl.startsWith('http')) {
                        nextUrl = new URL(nextUrl, currentUrl).href;
                    }
                    follow(nextUrl);
                } else {
                    resolve(currentUrl);
                }
            });

            req.on('error', reject);
            req.end();
        }

        follow(inputUrl);
    });
}

function extractHexCID(resolvedUrl) {
    const match = resolvedUrl.match(/!1s(0x[0-9a-fA-F]+:0x[0-9a-fA-F]+)/);
    return match ? match[1] : null;
}

function buildReviewUrl(hexCID) {
    return `https://www.google.com/maps/place//data=!4m3!3m2!1s${hexCID}!12e1`;
}

// ============ TEST ============
async function main() {
    const gmbUrl = "https://maps.app.goo.gl/yvBRbChDD1dDjwUe6";

    console.log("=".repeat(60));
    console.log("GMB Share URL -> Write Review Link Generator");
    console.log("=".repeat(60));
    console.log(`\nInput URL: ${gmbUrl}\n`);

    try {
        // Step 1: Resolve
        const resolvedUrl = await resolveRedirects(gmbUrl);
        console.log(`Resolved URL:\n${resolvedUrl}\n`);

        // Step 2: Extract HEX CID
        const hexCID = extractHexCID(resolvedUrl);
        if (!hexCID) {
            console.log("ERROR: HEX CID not found in URL");
            return;
        }
        console.log(`HEX CID: ${hexCID}\n`);

        // Step 3: Build review URL
        const reviewUrl = buildReviewUrl(hexCID);
        console.log(`Write Review URL:\n${reviewUrl}`);
    } catch (err) {
        console.error(`ERROR: ${err.message}`);
    }
}

main();
