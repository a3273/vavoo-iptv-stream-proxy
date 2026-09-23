const { Command } = require('commander');
const crypto = require('node:crypto');
const express = require('express');
const { Readable } = require('node:stream');
const { pipeline } = require('node:stream/promises');
const NodeCache = require('node-cache');

const program = new Command();

program
    .name('vavoo-iptv-stream-proxy')
    .description('Local proxy for Vavoo IPTV streams')
    .option('--http-host <host>', 'Local HTTP host', '0.0.0.0')
    .option('--http-port <port>', 'Local HTTP port', '8888')
    .option('--vavoo-language <language>', 'Language sent to Vavoo APIs', 'de')
    .option('--vavoo-region <region>', 'Region sent to Vavoo APIs', 'US')
    .option('--vavoo-url-list <selection>', 'URL list: primary, fallback, both', 'both')
    .option('--redirect', 'Redirect VAVOO user agents directly to upstream URLs', false)
    .parse(process.argv);

const options = program.opts();

const app = express();

/*
 * IMPORTANT FOR ONEBIT
 * Use the PORT supplied by the platform.
 * Fall back to 8888 when running locally.
 */
const httpHost = process.env.HOST || options.httpHost || '0.0.0.0';
const port = Number(process.env.PORT) || Number(options.httpPort) || 8888;

const currentLanguage = options.vavooLanguage;
const currentRegion = options.vavooRegion;
const vavooUrlList = options.vavooUrlList;
const redirect = Boolean(options.redirect);

function getBaseSites(selection) {
    const normalized = String(selection || 'both').trim().toLowerCase();

    if (normalized === 'primary') {
        return ['https://vavoo.to'];
    }

    if (normalized === 'fallback') {
        return ['https://kool.to'];
    }

    return ['https://vavoo.to', 'https://kool.to'];
}

const baseSites = getBaseSites(vavooUrlList);

const cache = new NodeCache();

process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

const CHANNELS_CACHE_KEY = 'vavoo_channels';
const SIGNATURE_CACHE_KEY = 'vavoo_addon_sig';

const COUNTRY_SEPARATORS = ['➾', '⟾', '->', '→', '»', '›'];

const PING_URLS = [
    'https://www.vavoo.tv/api/app/ping'
];

/* ---------------------------------------------------------
 * LOCAL URL
 * --------------------------------------------------------- */

function getLocalBaseUrl() {
    return `http://${httpHost}:${port}`;
}

/* ---------------------------------------------------------
 * HOME PAGE
 * --------------------------------------------------------- */

function buildHomePage() {
    const baseUrl = getLocalBaseUrl();

    return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Vavoo Proxy</title>
<style>
body {
    margin: 0;
    padding: 30px;
    background: #111;
    color: #eee;
    font-family: Arial, sans-serif;
}
a {
    color: #7cc7ff;
    display: block;
    margin: 12px 0;
    word-break: break-all;
}
</style>
</head>
<body>

<h1>Vavoo Proxy</h1>

<a href="/countries">/countries</a>
<a href="/channels.m3u8">/channels.m3u8</a>
<a href="/channels.m3u8?country=Italy">Italy M3U</a>
<a href="/channels.m3u8?country=Germany">Germany M3U</a>
<a href="/channels.m3u8?country=France">France M3U</a>
<a href="/channels.m3u8?country=Spain">Spain M3U</a>
<a href="/channels.m3u8?country=United%20Kingdom">UK M3U</a>

</body>
</html>`;
}

/* ---------------------------------------------------------
 * NORMALIZATION
 * --------------------------------------------------------- */

function normalize(value) {
    return String(value || '').trim().toLowerCase();
}

function normalizeChannelIdPart(value) {
    return normalize(value).replace(/\s+/g, ' ');
}

function getStableChannelId(name, country) {
    const seed = [
        normalizeChannelIdPart(country),
        normalizeChannelIdPart(name)
    ].join('|');

    return crypto
        .createHash('sha1')
        .update(seed)
        .digest('hex')
        .slice(0, 22);
}

function extractCountry(group) {
    const rawGroup = String(group || '').trim();

    if (!rawGroup) {
        return 'default';
    }

    for (const separator of COUNTRY_SEPARATORS) {
        if (rawGroup.includes(separator)) {
            return rawGroup.split(separator)[0].trim() || 'default';
        }
    }

    return rawGroup;
}

/* ---------------------------------------------------------
 * VAVOO HEADERS
 * --------------------------------------------------------- */

function getCatalogHeaders(signature) {
    return {
        'content-type': 'application/json; charset=utf-8',
        'mediahubmx-signature': signature,
        'user-agent': 'MediaHubMX/2',
        'accept': '*/*',
        'Accept-Language': currentLanguage,
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'close'
    };
}

function getStreamHeaders(req) {
    const headers = {
        'User-Agent': 'VAVOO/2.6',
        'Connection': 'close'
    };

    if (req.headers.range) {
        headers.Range = req.headers.range;
    }

    return headers;
}

/* ---------------------------------------------------------
 * HLS
 * --------------------------------------------------------- */

function getProxiedUpstreamUrl(req, upstreamUrl) {
    return `https://${req.headers.host}/hls-proxy?url=${encodeURIComponent(upstreamUrl)}`;
}

function setPlaylistHeaders(res) {
    res.type('application/vnd.apple.mpegurl');

    res.setHeader(
        'Cache-Control',
        'no-cache, no-store, must-revalidate'
    );

    res.setHeader('Pragma', 'no-cache');
}

function sendHlsMasterPlaylist(req, res, streamUrl) {
    setPlaylistHeaders(res);

    res.send(
        [
            '#EXTM3U',
            '#EXT-X-VERSION:3',
            '#EXT-X-STREAM-INF:BANDWIDTH=8000000',
            getProxiedUpstreamUrl(req, streamUrl)
        ].join('\n') + '\n'
    );
}

function isM3u8Url(upstreamUrl) {
    try {
        return new URL(upstreamUrl)
            .pathname
            .toLowerCase()
            .endsWith('.m3u8');
    } catch {
        return false;
    }
}

function isM3u8Response(upstreamUrl, contentType) {
    return (
        String(contentType || '')
            .toLowerCase()
            .includes('mpegurl')
        ||
        String(contentType || '')
            .toLowerCase()
            .includes('application/vnd.apple')
        ||
        isM3u8Url(upstreamUrl)
    );
}

function shouldRewritePlaylistUri(uri) {
    const trimmed = String(uri || '').trim();

    if (!trimmed) {
        return false;
    }

    return !/^(data|urn|skd):/i.test(trimmed);
}

function rewritePlaylistUri(req, baseUrl, uri) {
    if (!shouldRewritePlaylistUri(uri)) {
        return uri;
    }

    try {
        return getProxiedUpstreamUrl(
            req,
            new URL(uri, baseUrl).toString()
        );
    } catch {
        return uri;
    }
}

function rewriteM3u8Playlist(req, upstreamUrl, playlist) {
    return String(playlist)
        .split(/\r?\n/)
        .map(function (line) {
            const trimmed = line.trim();

            if (!trimmed) {
                return line;
            }

            if (trimmed.startsWith('#')) {
                return line.replace(
                    /URI="([^"]+)"/g,
                    function (match, uri) {
                        return `URI="${rewritePlaylistUri(
                            req,
                            upstreamUrl,
                            uri
                        )}"`;
                    }
                );
            }

            return rewritePlaylistUri(
                req,
                upstreamUrl,
                trimmed
            );
        })
        .join('\n');
}

/* ---------------------------------------------------------
 * HTTP HELPERS
 * --------------------------------------------------------- */

function setUpstreamHeaders(res, upstream) {
    const contentType = upstream.headers.get('content-type');

    if (contentType) {
        res.setHeader('Content-Type', contentType);
    }

    const contentLength = upstream.headers.get('content-length');

    if (contentLength) {
        res.setHeader('Content-Length', contentLength);
    }

    const acceptRanges = upstream.headers.get('accept-ranges');

    if (acceptRanges) {
        res.setHeader('Accept-Ranges', acceptRanges);
    }

    const contentRange = upstream.headers.get('content-range');

    if (contentRange) {
        res.setHeader('Content-Range', contentRange);
    }
}

async function requestJson(requestOptions) {
    const response = await fetch(requestOptions.url, {
        method: requestOptions.method || 'GET',
        headers: requestOptions.headers,
        body: requestOptions.body
            ? JSON.stringify(requestOptions.body)
            : undefined,
        signal: AbortSignal.timeout(
            requestOptions.timeout || 30000
        )
    });

    const text = await response.text();

    let body;

    try {
        body = JSON.parse(text);
    } catch {
        body = text;
    }

    if (!response.ok) {
        const error = new Error(
            `HTTP ${response.status} for ${requestOptions.url}`
        );

        error.statusCode = response.status;
        error.body = body;

        throw error;
    }

    return body;
}

/* ---------------------------------------------------------
 * ADDON SIGNATURE
 * --------------------------------------------------------- */

function getPingPayload() {
    const currentTimestamp = Date.now();

    return {
        reason: 'app-focus',
        locale: currentLanguage,
        theme: 'dark',

        metadata: {
            device: {
                type: 'desktop',
                uniqueId: `node-${currentTimestamp}`
            },

            os: {
                name: 'linux',
                version: 'Linux',
                abis: ['x64'],
                host: 'node'
            },

            app: {
                platform: 'electron'
            },

            version: {
                package: 'tv.vavoo.app',
                binary: '3.1.8',
                js: '3.1.8'
            }
        },

        appFocusTime: 0,
        playerActive: false,
        playDuration: 0,
        devMode: false,
        hasAddon: true,
        castConnected: false,

        package: 'tv.vavoo.app',
        version: '3.1.8',
        process: 'app',

        firstAppStart: currentTimestamp,
        lastAppStart: currentTimestamp,

        ipLocation: null,
        adblockEnabled: true,

        proxy: {
            supported: ['ss'],
            engine: 'Mu',
            enabled: false,
            autoServer: true
        },

        iap: {
            supported: false
        }
    };
}

async function getAddonSignature() {
    const cached = cache.get(SIGNATURE_CACHE_KEY);

    if (cached) {
        return cached;
    }

    const payload = getPingPayload();

    for (const url of PING_URLS) {
        try {
            console.log(`[vavoo] requesting addonSig from ${url}`);

            const body = await requestJson({
                method: 'POST',
                url,
                body: payload
            });

            const signature = body?.addonSig;

            if (signature) {
                cache.set(
                    SIGNATURE_CACHE_KEY,
                    signature,
                    300
                );

                console.log('[vavoo] addonSig obtained');

                return signature;
            }

            console.log('[vavoo] addonSig missing in response');

        } catch (error) {
            console.log(
                `[vavoo] addonSig request failed: ${error.message}`
            );
        }
    }

    throw new Error('Unable to obtain addonSig');
}

/* ---------------------------------------------------------
 * CATALOG
 * --------------------------------------------------------- */

function mapCatalogItem(item) {
    const name = item.name || 'Unknown Channel';
    const country = extractCountry(item.group);

    return {
        id: getStableChannelId(name, country),
        url: item.url,
        name,
        logo: item.logo || '',
        group: item.group || '',
        country
    };
}

async function loadCatalogFromBase(baseUrl, signature) {
    const catalogUrl =
        `${baseUrl.replace(/\/$/, '')}/mediahubmx-catalog.json`;

    console.log(
        `[vavoo] loading catalog from ${catalogUrl}`
    );

    const headers = getCatalogHeaders(signature);

    const channels = [];

    let cursor = null;

    while (true) {
        const body = await requestJson({
            method: 'POST',
            url: catalogUrl,
            headers,
            body: {
                language: currentLanguage,
                region: currentRegion,
                catalogId: 'iptv',
                id: 'iptv',
                adult: false,
                search: '',
                sort: '',
                filter: {},
                cursor,
                clientVersion: '3.0.2'
            }
        });

        const items = Array.isArray(body?.items)
            ? body.items
            : [];

        console.log(
            `[vavoo] catalog response: ${items.length} items`
        );

        for (const item of items) {
            if (
                item?.type === 'iptv' &&
                item?.url
            ) {
                channels.push(
                    mapCatalogItem(item)
                );
            }
        }

        if (!body?.nextCursor) {
            break;
        }

        cursor = body.nextCursor;
    }

    return channels;
}

async function getChannels(forceRefresh = false) {
    if (forceRefresh) {
        cache.del(CHANNELS_CACHE_KEY);
    }

    const cached = cache.get(CHANNELS_CACHE_KEY);

    if (cached) {
        console.log(
            `[vavoo] using cached channels: ${cached.length}`
        );

        return cached;
    }

    const signature = await getAddonSignature();

    for (const baseUrl of baseSites) {
        try {
            const channels =
                await loadCatalogFromBase(
                    baseUrl,
                    signature
                );

            cache.set(
                CHANNELS_CACHE_KEY,
                channels,
                300
            );

            console.log(
                `[vavoo] channels loaded from ${baseUrl}: ${channels.length}`
            );

            return channels;

        } catch (error) {
            console.log(
                `[vavoo] catalog load failed for ${baseUrl}: ${error.message}`
            );
        }
    }

    throw new Error(
        'Unable to load channel catalog'
    );
}

async function getChannelsByCountry(country) {
    const channels = await getChannels();

    return channels.filter(
        channel =>
            normalize(channel.country) ===
            normalize(country)
    );
}

async function getCountries() {
    const channels = await getChannels();

    return [
        ...new Set(
            channels
                .map(channel => channel.country)
                .filter(
                    country =>
                        country &&
                        normalize(country) !== 'default'
                )
        )
    ].sort(
        (left, right) =>
            left.localeCompare(right)
    );
}

async function findChannelById(id) {
    const channels = await getChannels();

    return channels.find(
        channel =>
            String(channel.id) === String(id)
    );
}

/* ---------------------------------------------------------
 * STREAM
 * --------------------------------------------------------- */

function normalizeStreamId(id) {
    return String(id || '').split('|')[0];
}

async function resolveStreamUrl(channel) {
    const signature = await getAddonSignature();

    for (const baseUrl of baseSites) {
        const resolveUrl =
            `${baseUrl.replace(/\/$/, '')}/mediahubmx-resolve.json`;

        try {
            console.log(
                `[vavoo] resolving channel "${channel.name}" using ${baseUrl}`
            );

            const body = await requestJson({
                method: 'POST',
                url: resolveUrl,
                headers: getCatalogHeaders(signature),
                body: {
                    language: currentLanguage,
                    region: currentRegion,
                    url: channel.url,
                    clientVersion: '3.0.2'
                }
            });

            if (
                Array.isArray(body) &&
                body[0]?.url
            ) {
                return body[0].url;
            }

            if (body?.url) {
                return body.url;
            }

            if (body?.streamUrl) {
                return body.streamUrl;
            }

        } catch (error) {
            console.log(
                `[vavoo] resolve failed for ${baseUrl}: ${error.message}`
            );
        }
    }

    throw new Error(
        `Unable to resolve stream for channel ${channel.name}`
    );
}

/* ---------------------------------------------------------
 * STREAM PROXY
 * --------------------------------------------------------- */

async function proxyStream(
    req,
    res,
    streamUrl,
    channelName
) {
    const connId =
        `${req.socket.remoteAddress}`;

    const controller =
        new AbortController();

    req.socket.on('close', function () {
        controller.abort();
    });

    try {
        console.log(
            `[${connId}] connecting to stream "${channelName}"`
        );

        const upstream = await fetch(
            streamUrl,
            {
                signal: controller.signal,
                headers: getStreamHeaders(req)
            }
        );

        if (
            !upstream.ok ||
            !upstream.body
        ) {
            throw new Error(
                `upstream returned HTTP ${upstream.status}`
            );
        }

        const contentType =
            upstream.headers.get(
                'content-type'
            );

        console.log(
            `[${connId}] stream connected status=${upstream.status} type="${contentType || 'unknown'}"`
        );

        if (
            isM3u8Response(
                streamUrl,
                contentType
            )
        ) {
            const playlist =
                await upstream.text();

            const rewrittenPlaylist =
                rewriteM3u8Playlist(
                    req,
                    streamUrl,
                    playlist
                );

            setPlaylistHeaders(res);

            res.send(rewrittenPlaylist);

            return;
        }

        setUpstreamHeaders(
            res,
            upstream
        );

        await pipeline(
            Readable.fromWeb(
                upstream.body
            ),
            res
        );

    } catch (error) {

        if (controller.signal.aborted) {
            return;
        }

        console.log(
            `[${connId}] stream error "${channelName}": ${error.message}`
        );

        if (!res.headersSent) {
            res
                .status(400)
                .send(
                    `stream error: ${error.message}`
                );
        }
    }
}

/* ---------------------------------------------------------
 * HLS PROXY
 * --------------------------------------------------------- */

async function proxyUpstreamUrl(
    req,
    res,
    upstreamUrl
) {
    const connId =
        `${req.socket.remoteAddress}`;

    const controller =
        new AbortController();

    req.socket.on('close', function () {
        controller.abort();
    });

    try {
        const upstream =
            await fetch(
                upstreamUrl,
                {
                    signal: controller.signal,
                    headers: getStreamHeaders(req)
                }
            );

        if (
            !upstream.ok ||
            !upstream.body
        ) {
            throw new Error(
                `upstream returned HTTP ${upstream.status}`
            );
        }

        const contentType =
            upstream.headers.get(
                'content-type'
            );

        if (
            isM3u8Response(
                upstreamUrl,
                contentType
            )
        ) {
            const playlist =
                await upstream.text();

            const rewrittenPlaylist =
                rewriteM3u8Playlist(
                    req,
                    upstreamUrl,
                    playlist
                );

            setPlaylistHeaders(res);

            res.send(rewrittenPlaylist);

            return;
        }

        setUpstreamHeaders(
            res,
            upstream
        );

        await pipeline(
            Readable.fromWeb(
                upstream.body
            ),
            res
        );

    } catch (error) {

        if (controller.signal.aborted) {
            return;
        }

        console.log(
            `[${connId}] hls proxy error: ${error.message}`
        );

        if (!res.headersSent) {
            res
                .status(400)
                .send(
                    `upstream proxy error: ${error.message}`
                );
        }
    }
}

/* ---------------------------------------------------------
 * ROUTES
 * --------------------------------------------------------- */

app.get('/', function (req, res) {
    res
        .type('html')
        .send(buildHomePage());
});

/*
 * HEALTH CHECK
 * Useful for OneBit.
 */
app.get('/health', function (req, res) {
    res.json({
        status: 'ok',
        service: 'vavoo-iptv-stream-proxy',
        port,
        host: httpHost,
        node: process.version,
        time: new Date().toISOString()
    });
});

app.get('/countries', async function (req, res) {
    try {
        console.log('[HTTP] GET /countries');

        const countries =
            await getCountries();

        res.json(countries);

    } catch (error) {

        console.log(
            '[vavoo] countries error:',
            error.message
        );

        res
            .status(500)
            .send(error.message);
    }
});

app.get('/channels.m3u8', async function (req, res) {
    try {
        console.log(
            `[HTTP] GET /channels.m3u8 country=${req.query.country || 'ALL'}`
        );

        const country =
            req.query.country;

        const channels =
            country
                ? await getChannelsByCountry(country)
                : await getChannels();

        console.log(
            `[vavoo] generating M3U: ${channels.length} channels`
        );

        const output = ['#EXTM3U'];

        for (const channel of channels) {

            output.push(
                `#EXTINF:-1 tvg-name="${channel.name}" group-title="${channel.country}" tvg-logo="${channel.logo}" tvg-id="${channel.name}",${channel.name}`
            );

            output.push(
                '#EXTVLCOPT:http-user-agent=VAVOO/2.6'
            );

            output.push(
                '#EXTVLCOPT:no-ssl-verify'
            );

            output.push(
                `https://${req.headers.host}/stream/${encodeURIComponent(channel.id)}`
            );
        }

        setPlaylistHeaders(res);

        res.send(
            output.join('\n')
        );

    } catch (error) {

        console.log(
            '[vavoo] channels.m3u8 error:',
            error.message
        );

        res
            .status(500)
            .send(error.message);
    }
});

app.get('/hls-proxy', async function (req, res) {

    const upstreamUrl =
        req.query.url;

    if (!upstreamUrl) {
        res
            .status(400)
            .send('missing url');

        return;
    }

    try {

        const parsedUrl =
            new URL(upstreamUrl);

        if (
            !['http:', 'https:']
                .includes(parsedUrl.protocol)
        ) {
            res
                .status(400)
                .send(
                    'unsupported upstream protocol'
                );

            return;
        }

        await proxyUpstreamUrl(
            req,
            res,
            parsedUrl.toString()
        );

    } catch (error) {

        res
            .status(400)
            .send(
                `invalid upstream url: ${error.message}`
            );
    }
});

app.get('/stream/:id', async function (req, res) {

    const connId =
        `${req.socket.remoteAddress}`;

    const userAgent =
        req.headers['user-agent'] ||
        'unknown';

    try {

        console.log(
            `[${connId}] GET /stream/${req.params.id} UA="${userAgent}"`
        );

        const channelId =
            normalizeStreamId(
                req.params.id
            );

        const channel =
            await findChannelById(
                channelId
            );

        if (!channel) {

            console.log(
                `[${connId}] channel not found: ${channelId}`
            );

            res
                .status(404)
                .send(
                    `unknown channel: ${channelId}`
                );

            return;
        }

        console.log(
            `[${connId}] channel found: ${channel.name}`
        );

        const streamUrl =
            await resolveStreamUrl(
                channel
            );

        console.log(
            `[${connId}] stream resolved for "${channel.name}"`
        );

        if (
            redirect &&
            userAgent
                .toLowerCase()
                .includes('vavoo')
        ) {
            res.redirect(streamUrl);
            return;
        }

        if (
            isM3u8Url(streamUrl)
        ) {
            sendHlsMasterPlaylist(
                req,
                res,
                streamUrl
            );

            return;
        }

        await proxyStream(
            req,
            res,
            streamUrl,
            channel.name
        );

    } catch (error) {

        console.log(
            `[${connId}] playback error:`,
            error.message
        );

        if (!res.headersSent) {
            res
                .status(500)
                .send(error.message);
        }
    }
});

/* ---------------------------------------------------------
 * START SERVER
 * --------------------------------------------------------- */

console.log('----------------------------------------');
console.log('Vavoo IPTV Stream Proxy');
console.log('Node:', process.version);
console.log('HOST:', httpHost);
console.log('PORT:', port);
console.log('ENV PORT:', process.env.PORT || 'not set');
console.log('Region:', currentRegion);
console.log('Language:', currentLanguage);
console.log('Base sites:', baseSites.join(', '));
console.log('----------------------------------------');

const server = app.listen(
    port,
    httpHost,
    function () {

        console.log(
            `Server listening on ${httpHost}:${port}`
        );

        console.log(
            `Health: http://${httpHost}:${port}/health`
        );

        console.log(
            `M3U: http://${httpHost}:${port}/channels.m3u8`
        );

        console.log(
            `Countries: http://${httpHost}:${port}/countries`
        );
    }
);

server.on('error', function (error) {

    console.error(
        '[SERVER ERROR]',
        error
    );

    process.exit(1);
});

process.on('uncaughtException', function (error) {

    console.error(
        '[UNCAUGHT EXCEPTION]',
        error
    );
});

process.on('unhandledRejection', function (error) {

    console.error(
        '[UNHANDLED REJECTION]',
        error
    );
});
