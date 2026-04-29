// app.js v3.5 - Top Traders + Tokens + Investor Classification + Behavior Filter + Guide
// Cambios 29-abr-2026 (v3.5):
// - Ocultas columnas 5M y TXNS (eran placeholder/no confiables).
// - Eliminado filtro "Txns" (mapeaba a volumen).
// - LAST SEEN → LAST SYNC con tooltip.
// - Stat "txns/24h" → "wallets refreshed (24h)".
// - Tooltips de freshness en headers cacheados.
const API_BASE = '/api/';
let currentView = 'traders';
let currentSort = 'pnl';
let currentOrder = 'desc';
let currentTimeRange = 'all';
let currentBehaviorFilter = 'all';
let searchTerm = '';
let allData = [];
const REFRESH_INTERVAL = 15000;

// ============================================================================
// Investor Guide Toggle
// ============================================================================
document.getElementById('guideToggle').addEventListener('click', () => {
    const panel = document.getElementById('guidePanel');
    const btn = document.getElementById('guideToggle');
    panel.classList.toggle('open');
    btn.classList.toggle('active');
});

// ============================================================================
// View Configuration
// ============================================================================
const VIEW_CONFIG = {
    traders: {
        filters: [
            { label: 'P&L', sort: 'pnl' },
            { label: 'Win Rate', sort: 'winrate' },
            { label: 'ROI %', sort: 'roi' },
            { label: 'Score', sort: 'score' },
            { label: 'Trades', sort: 'trades' },
            { label: 'Unrealized', sort: 'invested' },
            { label: 'Best Token', sort: 'besttrade' },
        ],
        timeFilters: [
            { label: '1H', value: '1h' },
            { label: '6H', value: '6h' },
            { label: '24H', value: '24h' },
            { label: '7D', value: '7d' },
            { label: '30D', value: '30d' },
            { label: 'ALL', value: 'all' },
        ],
        defaultSort: 'pnl',
        headers: `<tr>
            <th class="th-token">TRADER</th>
            <th title="Total profit & loss in USD (cached, 30min–several hours)">P&L (USD)</th>
            <th title="Win rate from cached PnL (30min–several hours)">WIN RATE</th>
            <th title="ROI % from cached PnL">ROI %</th>
            <th title="Investor score 0–100, derived by our classifier">SCORE</th>
            <th title="Trade count from cached PnL">TRADES</th>
            <th title="Unrealized PnL in USD">UNREALIZED (USD)</th>
            <th title="Realized PnL in USD">REALIZED (USD)</th>
            <th title="Best token by PnL in our cache (not necessarily a single trade)">BEST TOKEN (USD)</th>
            <th title="Worst token by PnL in our cache">WORST TOKEN (USD)</th>
            <th title="Distinct tokens where this wallet appears as top trader in our cache">TOKENS</th>
            <th title="Last time our worker synced this wallet (NOT last on-chain activity)">LAST SYNC</th>
        </tr>`,
    },
    tokens: {
        filters: [
            { label: 'Volume', sort: 'volume' },
            { label: 'MCap', sort: 'mcap' },
            { label: 'Gainers (24h)', sort: 'change24h' },
            { label: 'Investors', sort: 'investors' },
            // 'Txns' eliminado: mapeaba a volume_24h en backend, era engañoso.
            { label: 'Newest', sort: 'age' },
        ],
        timeFilters: [],
        defaultSort: 'volume',
        // 5M y TXNS ocultos. Si más adelante se llenan, vuelven aquí.
        headers: `<tr>
            <th class="th-token">TOKEN</th>
            <th title="Last cached price (5–15 min)">PRICE</th>
            <th title="Time since token was first detected">AGE</th>
            <th title="Classified investors holding positions in this token">INVESTORS</th>
            <th title="24h trading volume (cached)">VOLUME</th>
            <th title="Holder count (cached, ~5–15 min)">HOLDERS</th>
            <th title="Price change vs 1h ago, computed from local price history">1H</th>
            <th title="Price change vs 6h ago, computed from local price history">6H</th>
            <th title="Price change vs 24h ago, computed from local price history">24H</th>
            <th title="Liquidity (cached)">LIQUIDITY</th>
            <th title="Market cap (cached)">MCAP</th>
        </tr>`,
    },
};

// ============================================================================
// Formatting Functions
// ============================================================================
function formatUSD(val) {
    if (val === null || val === undefined || val === 0) return '$0';
    const abs = Math.abs(val);
    const sign = val < 0 ? '-' : '';
    let body;
    if (abs >= 1e9) body = (abs / 1e9).toFixed(2) + 'B';
    else if (abs >= 1e6) body = (abs / 1e6).toFixed(2) + 'M';
    else if (abs >= 1e3) body = (abs / 1e3).toFixed(1) + 'K';
    else if (abs >= 1) body = abs.toFixed(2);
    else body = abs.toFixed(4);
    return `${sign}$${body}`;
}

function formatPrice(price) {
    if (!price) price = 0;
    if (price < 0.0000001) {
        const exp = Math.floor(Math.log10(price));
        const mantissa = price / Math.pow(10, exp);
        const zeros = Math.abs(exp - 1);
        const significant = mantissa.toFixed(2);
        return `0.0<sub>${zeros}</sub>${significant.replace('0.', '').replace('.', '')}`;
    }
    if (price < 0.00001) return price.toFixed(10);
    if (price < 0.001) return price.toFixed(6);
    if (price < 1) return price.toFixed(4);
    return price.toLocaleString('en-US', { maximumFractionDigits: 2 });
}

function formatCompact(value) {
    if (!value) value = 0;
    if (value >= 1e9) return (value / 1e9).toFixed(1) + 'B';
    if (value >= 1e6) return (value / 1e6).toFixed(1) + 'M';
    if (value >= 1e3) return (value / 1e3).toFixed(0) + 'K';
    return value.toFixed(0);
}

function formatNumber(num) {
    if (!num) return '0';
    return num.toLocaleString('en-US');
}

function formatPercent(val) {
    if (val === null || val === undefined) return `<span class="neutral">–</span>`;
    const cls = val > 0 ? 'positive' : (val < 0 ? 'negative' : 'neutral');
    let formatted;
    if (Math.abs(val) >= 1000) formatted = val.toLocaleString('en-US', { maximumFractionDigits: 0 });
    else if (Math.abs(val) >= 100) formatted = val.toFixed(0);
    else formatted = val.toFixed(2);
    return `<span class="${cls}">${formatted}%</span>`;
}

function formatPNL(val) {
    if (val === null || val === undefined || val === 0) return `<span class="neutral">$0</span>`;
    const cls = val > 0 ? 'pnl-positive' : 'pnl-negative';
    const sign = val > 0 ? '+' : '';
    return `<span class="${cls}">${sign}${formatUSD(val)}</span>`;
}

function formatPNLBig(val) {
    if (val === null || val === undefined || val === 0) return `<span class="neutral">$0</span>`;
    const cls = val > 0 ? 'positive' : 'negative';
    const sign = val > 0 ? '+' : '';
    return `<span class="pnl-big ${cls}">${sign}${formatUSD(val)}</span>`;
}

function formatWinRate(rate) {
    const fillClass = rate >= 60 ? 'winrate-high' : rate >= 40 ? 'winrate-mid' : 'winrate-low';
    return `<div class="winrate-container"><span>${rate.toFixed(1)}%</span><div class="winrate-bar"><div class="winrate-fill ${fillClass}" style="width:${Math.min(rate, 100)}%"></div></div></div>`;
}

function formatROI(val) {
    if (!val) return `<span class="neutral">0%</span>`;
    const cls = val > 0 ? 'positive' : 'negative';
    const sign = val > 0 ? '+' : '';
    return `<span class="${cls}">${sign}${val.toFixed(1)}%</span>`;
}

function renderTags(tags) {
    if (!tags) return '';
    return tags.split(',').map(t => {
        const tag = t.trim().toLowerCase();
        let cls = 'tag';
        if (tag.includes('whale')) cls = 'tag-whale';
        else if (tag.includes('bot')) cls = 'tag-bot';
        else if (tag.includes('insider')) cls = 'tag-insider';
        return `<span class="${cls}">${tag}</span>`;
    }).join('');
}

// ============================================================================
// Classification Formatting
// ============================================================================
function formatInvestorBadge(classification) {
    if (!classification || !classification.investortype || classification.investortype === 'unclassified') return '';
    const c = classification;
    const typeConfig = {
        'elite': { cls: 'inv-elite', icon: '⭐' },
        'profitable': { cls: 'inv-profitable', icon: '💰' },
        'regular': { cls: 'inv-regular', icon: '👤' },
        'bot-profitable': { cls: 'inv-bot-profit', icon: '🤖' },
        'bot-regular': { cls: 'inv-bot', icon: '🤖' },
        'casual': { cls: 'inv-casual', icon: '🎲' },
        'losing': { cls: 'inv-losing', icon: '📉' },
    };
    const cfg = typeConfig[c.investortype] || { cls: '', icon: '' };
    let html = `<span class="tag ${cfg.cls}">${cfg.icon} ${c.investortype.replace('-', ' ')}</span>`;
    if (c.investorscore > 0) {
        html += `<span class="inv-score">${c.investorscore}</span>`;
    }
    return html;
}

function formatInvestorScore(score) {
    if (!score || score === 0) return `<span class="neutral">–</span>`;
    let cls = 'inv-score-low';
    if (score >= 70) cls = 'inv-score-high';
    else if (score >= 40) cls = 'inv-score-mid';
    return `<span class="inv-score-cell ${cls}">${score}</span>`;
}

function formatInvestors(investors) {
    if (!investors || !investors.total) return `<span class="neutral">–</span>`;
    const inv = investors;
    let badges = [];
    if (inv.elite > 0) badges.push(`<span class="inv-badge inv-elite" title="${inv.elite} Elite Traders">⭐${inv.elite}</span>`);
    if (inv.profitable > 0) badges.push(`<span class="inv-badge inv-profitable" title="${inv.profitable} Profitable Traders">💰${inv.profitable}</span>`);
    if (inv.regular > 0) badges.push(`<span class="inv-badge inv-regular" title="${inv.regular} Regular Traders">👤${inv.regular}</span>`);
    const humanPct = inv.total > 0 ? Math.round((inv.humans / inv.total) * 100) : 0;
    let qualityClass = 'inv-quality-high';
    if (humanPct < 40) qualityClass = 'inv-quality-low';
    else if (humanPct < 70) qualityClass = 'inv-quality-mid';
    return `<div class="investors-cell">
        <div class="inv-count">${inv.total}</div>
        <div class="inv-badges">${badges.join('')}</div>
        <div class="inv-bar ${qualityClass}" title="${humanPct}% humans, ${100 - humanPct}% bots">
            <div class="inv-bar-fill" style="width:${humanPct}%"></div>
        </div>
    </div>`;
}

// ============================================================================
// Behavior Filter Logic (sin cambios)
// ============================================================================
function filterByBehavior(data, behavior) {
    if (behavior === 'all') return data;
    switch (behavior) {
        case 'human': return data.filter(t => t.classification && t.classification.behavior === 'human');
        case 'bot': return data.filter(t => t.classification && t.classification.behavior === 'bot');
        case 'suspicious': return data.filter(t => t.classification && t.classification.behavior === 'suspicious');
        case 'elite': return data.filter(t => t.classification && t.classification.investortype === 'elite');
        case 'profitable': return data.filter(t => t.classification && t.classification.investortype === 'profitable');
        case 'regular': return data.filter(t => t.classification && t.classification.investortype === 'regular');
        case 'bot-profitable': return data.filter(t => t.classification && t.classification.investortype === 'bot-profitable');
        case 'bot-regular': return data.filter(t => t.classification && t.classification.investortype === 'bot-regular');
        case 'losing': return data.filter(t => t.classification && t.classification.investortype === 'losing');
        default: return data;
    }
}

function getFilteredTraders() {
    let filtered = [...allData];
    filtered = filterByBehavior(filtered, currentBehaviorFilter);
    if (searchTerm) {
        const term = searchTerm.toLowerCase();
        filtered = filtered.filter(t =>
            t.walletaddress.toLowerCase().includes(term) ||
            (t.tags && t.tags.toLowerCase().includes(term)) ||
            (t.classification && t.classification.investortype && t.classification.investortype.toLowerCase().includes(term))
        );
    }
    return filtered;
}

function updateBehaviorDropdownCounts() {
    const sel = document.getElementById('behaviorFilter');
    if (!sel || !allData.length) return;
    const counts = {
        all: allData.length,
        human: allData.filter(t => t.classification && t.classification.behavior === 'human').length,
        bot: allData.filter(t => t.classification && t.classification.behavior === 'bot').length,
        suspicious: allData.filter(t => t.classification && t.classification.behavior === 'suspicious').length,
        elite: allData.filter(t => t.classification && t.classification.investortype === 'elite').length,
        profitable: allData.filter(t => t.classification && t.classification.investortype === 'profitable').length,
        regular: allData.filter(t => t.classification && t.classification.investortype === 'regular').length,
        'bot-profitable': allData.filter(t => t.classification && t.classification.investortype === 'bot-profitable').length,
        'bot-regular': allData.filter(t => t.classification && t.classification.investortype === 'bot-regular').length,
        losing: allData.filter(t => t.classification && t.classification.investortype === 'losing').length,
    };
    for (const opt of sel.options) {
        const v = opt.value;
        if (counts[v] !== undefined) {
            const base = opt.getAttribute('data-label') || opt.textContent.replace(/\s*\(\d+\)/, '');
            opt.setAttribute('data-label', base);
            opt.textContent = `${base} (${counts[v]})`;
        }
    }
}

// ============================================================================
// Data Fetching
// ============================================================================
async function fetchData() {
    try {
        if (currentView === 'traders') {
            const url = `${API_BASE}top-traders?sort=${currentSort}&order=${currentOrder}&limit=50&mintrades=3&timerange=${currentTimeRange}`;
            const res = await fetch(url);
            const data = await res.json();
            if (data.success) {
                allData = data.traders;
                updateBehaviorDropdownCounts();
                renderTraders(getFilteredTraders());
                updateStatus(true, data.count, 'traders');
            } else {
                allData = [];
                renderTraders([]);
                updateStatus(false, 0, 'traders', data.error);
            }
        } else {
            const url = `${API_BASE}tokens?sort=${currentSort}&order=${currentOrder}&limit=50`;
            const res = await fetch(url);
            const data = await res.json();
            if (data.success) {
                allData = data.tokens;
                renderTokens(allData);
                updateStatus(true, data.count, 'tokens');
            } else {
                allData = [];
                renderTokens([]);
                updateStatus(false, 0, 'tokens', data.error);
            }
        }
    } catch (err) {
        console.error('Error:', err);
        allData = [];
        if (currentView === 'traders') renderTraders([]);
        else renderTokens([]);
        updateStatus(false, 0, currentView, err.message);
    }
}

async function fetchStats() {
    try {
        const res = await fetch(`${API_BASE}stats`);
        const data = await res.json();
        if (data.success) {
            document.getElementById('statTokens').textContent = `${data.totaltokens.toLocaleString()} tokens`;
            document.getElementById('statWallets').textContent = `${data.totalwallets.toLocaleString()} wallets`;
            // Honesto: el backend cuenta wallets refrescadas en últimas 24h, no transacciones.
            document.getElementById('statTxns').textContent = `${data.transactions24h.toLocaleString()} wallets refreshed (24h)`;
            if (data.classifications && data.classifications.total > 0) {
                const c = data.classifications;
                const classEl = document.getElementById('statClassifications');
                if (classEl) {
                    classEl.textContent = `${c.total} classified (${c.humans}👤 ${c.bots}🤖 ${c.elite}⭐)`;
                }
            }
        }
    } catch (err) { /* silent */ }
}

// ============================================================================
// Render Top Traders (colspan 12)
// ============================================================================
function renderTraders(traders) {
    const tbody = document.getElementById('tableBody');
    if (!traders || traders.length === 0) {
        tbody.innerHTML = `<tr><td colspan="12" style="text-align:center;padding:40px;color:#5c5e64">${searchTerm || currentBehaviorFilter !== 'all' ? 'No traders found with that filter' : 'No traders with enough trades'}</td></tr>`;
        return;
    }
    tbody.innerHTML = traders.map((t, i) => {
        const rank = i + 1;
        const rankClass = rank === 1 ? 'gold' : rank === 2 ? 'silver' : rank === 3 ? 'bronze' : '';
        const avatarClass = rank === 1 ? 'top1' : rank === 2 ? 'top2' : rank === 3 ? 'top3' : 'normal';
        const rankIcon = rank === 1 ? '🥇' : rank === 2 ? '🥈' : rank === 3 ? '🥉' : '';
        const shortAddr = `${t.walletaddress.slice(0, 6)}...${t.walletaddress.slice(-4)}`;
        const classificationBadge = formatInvestorBadge(t.classification);
        const score = t.classification ? t.classification.investorscore : 0;
        return `<tr onclick="window.open('https://solscan.io/account/${t.walletaddress}','_blank')" title="${t.walletaddress}">
            <td>
                <div class="trader-cell">
                    <span class="trader-rank ${rankClass}">${rank}</span>
                    <div class="trader-avatar ${avatarClass}">${rankIcon}</div>
                    <div class="trader-info">
                        <span class="trader-address">${shortAddr}</span>
                        <span class="trader-tags">${renderTags(t.tags)}${classificationBadge} · ${t.tokenstraded} tokens traded</span>
                    </div>
                </div>
            </td>
            <td>${formatPNLBig(t.totalpnl)}</td>
            <td>${formatWinRate(t.winrate)}</td>
            <td>${formatROI(t.roipercentage)}</td>
            <td>${formatInvestorScore(score)}</td>
            <td>${formatNumber(t.totaltrades)}</td>
            <td>${formatPNL(t.unrealizedpnl)}</td>
            <td>${formatPNL(t.totalrealized)}</td>
            <td>${formatPNL(t.besttrade)}</td>
            <td>${formatPNL(t.worsttrade)}</td>
            <td>${t.tokenstraded}</td>
            <td class="age" title="Last time our worker synced this wallet">${t.lastactivity}</td>
        </tr>`;
    }).join('');
}

// ============================================================================
// Render Tokens (colspan 11 — sin 5M ni TXNS)
// ============================================================================
function renderTokens(tokens) {
    const tbody = document.getElementById('tableBody');
    let filtered = tokens;
    if (searchTerm) {
        const term = searchTerm.toLowerCase();
        filtered = tokens.filter(t =>
            (t.name && t.name.toLowerCase().includes(term)) ||
            (t.symbol && t.symbol.toLowerCase().includes(term)) ||
            (t.mintaddress && t.mintaddress.toLowerCase().includes(term))
        );
    }
    if (!filtered || filtered.length === 0) {
        tbody.innerHTML = `<tr><td colspan="11" style="text-align:center;padding:40px;color:#5c5e64">${searchTerm ? 'No tokens found' : 'No data available'}</td></tr>`;
        return;
    }
    tbody.innerHTML = filtered.map((t, i) => {
        const imgHTML = t.imageurl ? `<img src="${t.imageurl}" alt="">` : '💎';
        const ammBadge = t.amm ? `<span class="badge badge-amm">${t.amm}</span>` : '';
        let displaySymbol = t.symbol || '???';
        let displayName = t.name || 'Unknown';
        return `<tr onclick="window.open('https://dexscreener.com/solana/${t.mintaddress}','_blank')" title="${t.mintaddress}">
            <td>
                <div class="token-cell">
                    <span class="token-rank">${i + 1}</span>
                    <div class="token-icon">${imgHTML}</div>
                    <div class="token-info">
                        <span class="token-name">${displaySymbol} <span style="display:inline-flex;gap:3px;margin-left:4px">${ammBadge}</span></span>
                        <span class="token-chain">${displayName}</span>
                    </div>
                </div>
            </td>
            <td class="price">${formatPrice(t.price)}</td>
            <td class="age">${t.age}</td>
            <td>${formatInvestors(t.investors)}</td>
            <td>${formatCompact(t.volume24h)}</td>
            <td>${formatNumber(t.makers)}</td>
            <td>${formatPercent(t.pct1h)}</td>
            <td>${formatPercent(t.pct6h)}</td>
            <td>${formatPercent(t.pct24h)}</td>
            <td>${formatCompact(t.liquidity)}</td>
            <td>${formatCompact(t.marketcap)}</td>
        </tr>`;
    }).join('');
}

// ============================================================================
// UI: View switching & filters
// ============================================================================
function switchView(view) {
    currentView = view;
    const config = VIEW_CONFIG[view];
    currentSort = config.defaultSort;
    currentOrder = 'desc';
    currentTimeRange = 'all';
    currentBehaviorFilter = 'all';
    searchTerm = '';
    document.getElementById('searchBox').value = '';
    allData = [];
    const colspan = view === 'traders' ? 12 : 11;
    document.getElementById('tableBody').innerHTML = `<tr><td colspan="${colspan}" style="text-align:center;padding:40px;color:#5c5e64">Loading data...</td></tr>`;

    document.querySelectorAll('.view-btn').forEach(b => b.classList.remove('active'));
    document.querySelector(`.view-btn[data-view="${view}"]`).classList.add('active');
    document.getElementById('tableHead').innerHTML = config.headers;

    const subfilters = document.getElementById('subfilters');
    let filtersHTML = '';
    filtersHTML += config.filters.map((f, i) =>
        `<button class="filter-btn sort-filter${i === 0 ? ' active' : ''}" data-sort="${f.sort}">${f.label}</button>`
    ).join('');

    if (config.timeFilters && config.timeFilters.length > 0) {
        filtersHTML += `<span style="border-left:1px solid #2a2d35;height:20px;margin:0 8px;align-self:center"></span>`;
        filtersHTML += config.timeFilters.map(tf =>
            `<button class="filter-btn time-filter${tf.value === 'all' ? ' active' : ''}" data-time="${tf.value}">${tf.label}</button>`
        ).join('');

        if (view === 'traders') {
            filtersHTML += `<span style="border-left:1px solid #2a2d35;height:20px;margin:0 8px;align-self:center"></span>`;
            filtersHTML += `<select id="behaviorFilter" class="behavior-dropdown">
                <option value="all" data-label="All Traders">All Traders</option>
                <optgroup label="By Behavior">
                    <option value="human" data-label="Humans">Humans</option>
                    <option value="bot" data-label="Bots">Bots</option>
                    <option value="suspicious" data-label="Suspicious">Suspicious</option>
                </optgroup>
                <optgroup label="By Investor Type">
                    <option value="elite" data-label="Elite">Elite</option>
                    <option value="profitable" data-label="Profitable">Profitable</option>
                    <option value="regular" data-label="Regular">Regular</option>
                    <option value="bot-profitable" data-label="Bot Profitable">Bot Profitable</option>
                    <option value="bot-regular" data-label="Bot Regular">Bot Regular</option>
                    <option value="losing" data-label="Losing">Losing</option>
                </optgroup>
            </select>`;
        }
    }

    subfilters.innerHTML = filtersHTML;

    subfilters.querySelectorAll('.sort-filter').forEach(btn => {
        btn.addEventListener('click', () => {
            subfilters.querySelectorAll('.sort-filter').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            const newSort = btn.dataset.sort;
            if (newSort === currentSort) {
                currentOrder = currentOrder === 'desc' ? 'asc' : 'desc';
            } else {
                currentSort = newSort;
                currentOrder = 'desc';
            }
            fetchData();
        });
    });

    subfilters.querySelectorAll('.time-filter').forEach(btn => {
        btn.addEventListener('click', () => {
            subfilters.querySelectorAll('.time-filter').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            currentTimeRange = btn.dataset.time;
            fetchData();
        });
    });

    const behaviorSelect = document.getElementById('behaviorFilter');
    if (behaviorSelect) {
        behaviorSelect.addEventListener('change', (e) => {
            currentBehaviorFilter = e.target.value;
            renderTraders(getFilteredTraders());
        });
    }

    document.getElementById('searchBox').placeholder = view === 'traders'
        ? 'Search wallet, tag or type...'
        : 'Search token or paste address...';

    fetchData();
}

function updateStatus(connected, count, type, error) {
    const statusEl = document.getElementById('connectionStatus');
    const updateEl = document.getElementById('lastUpdate');
    if (connected) {
        const label = type === 'traders' ? 'traders' : 'tokens';
        const timeLabel = currentView === 'traders' && currentTimeRange !== 'all' ? ` (${currentTimeRange.toUpperCase()})` : '';
        const behaviorLabel = currentView === 'traders' && currentBehaviorFilter !== 'all' ? ` [${currentBehaviorFilter}]` : '';
        statusEl.innerHTML = `<span class="status-dot connected"></span> Connected · ${count} ${label}${timeLabel}${behaviorLabel}`;
        updateEl.textContent = `Last update: ${new Date().toLocaleTimeString()}`;
    } else {
        statusEl.innerHTML = `<span class="status-dot error"></span> Error: ${error || 'no connection'}`;
    }
}

// ============================================================================
// Event Listeners & Init
// ============================================================================
document.querySelectorAll('.view-btn').forEach(btn => {
    btn.addEventListener('click', () => switchView(btn.dataset.view));
});

document.getElementById('searchBox').addEventListener('input', (e) => {
    searchTerm = e.target.value;
    if (currentView === 'traders') renderTraders(getFilteredTraders());
    else renderTokens(allData);
});

switchView('traders');
fetchStats();
setInterval(fetchData, REFRESH_INTERVAL);
setInterval(fetchStats, 60000);

console.log('✅ Memecoin Screener v3.5: hidden 5M/TXNS + LAST SYNC + honest stats label');