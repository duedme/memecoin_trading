// app.js v3 — Top Traders + Tokens + Investor Classification
const API_BASE = '/api';

let currentView = 'traders';
let currentSort = 'pnl';
let currentOrder = 'desc';
let currentTimeRange = 'all';
let searchTerm = '';
let allData = [];

const REFRESH_INTERVAL = 15000;

// ═══════════════════════════════════════
// View Configuration
// ═══════════════════════════════════════

const VIEW_CONFIG = {
    traders: {
        filters: [
            { label: 'P&L', sort: 'pnl' },
            { label: 'Win Rate', sort: 'win_rate' },
            { label: 'ROI', sort: 'roi' },
            { label: 'Score', sort: 'score' },
            { label: 'Trades', sort: 'trades' },
            { label: 'Invested', sort: 'invested' },
            { label: 'Best Trade', sort: 'best_trade' },
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
            <th>P&L (SOL)</th>
            <th>WIN RATE</th>
            <th>ROI %</th>
            <th>SCORE</th>
            <th>TRADES</th>
            <th>INVESTED</th>
            <th>REALIZED</th>
            <th>BEST TRADE</th>
            <th>WORST TRADE</th>
            <th>TOKENS</th>
            <th>LAST SEEN</th>
        </tr>`,
    },
    tokens: {
        filters: [
            { label: 'Volume', sort: 'volume' },
            { label: 'MCap', sort: 'mcap' },
            { label: 'Gainers', sort: 'change_24h' },
            { label: 'Investors', sort: 'investors' },
            { label: 'Txns', sort: 'txns' },
            { label: 'Newest', sort: 'age' },
        ],
        timeFilters: [],
        defaultSort: 'volume',
        headers: `<tr>
            <th class="th-token">TOKEN</th>
            <th>PRICE</th>
            <th>AGE</th>
            <th>INVESTORS</th>
            <th>TXNS</th>
            <th>VOLUME</th>
            <th>MAKERS</th>
            <th>5M</th>
            <th>1H</th>
            <th>6H</th>
            <th>24H</th>
            <th>LIQUIDITY</th>
            <th>MCAP</th>
        </tr>`,
    },
};


// ═══════════════════════════════════════
// Formatting Functions
// ═══════════════════════════════════════

function formatSOL(val) {
    if (!val || val === 0) return '0';
    if (Math.abs(val) >= 1000) return val.toFixed(1);
    if (Math.abs(val) >= 1) return val.toFixed(4);
    return val.toFixed(6);
}

function formatPrice(price) {
    if (!price || price === 0) return '0';
    if (price < 0.0000001) {
        const exp = Math.floor(Math.log10(price));
        const mantissa = price / Math.pow(10, exp);
        const zeros = Math.abs(exp) - 1;
        const significant = mantissa.toFixed(2);
        return `0.0<sub>${zeros}</sub>${significant.replace('0.', '').replace('.', '')}`;
    }
    if (price < 0.00001) return price.toFixed(10);
    if (price < 0.001) return price.toFixed(6);
    if (price < 1) return price.toFixed(4);
    return price.toLocaleString('en-US', { maximumFractionDigits: 2 });
}

function formatCompact(value) {
    if (!value || value === 0) return '0';
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
    if (val === null || val === undefined) return '<span class="neutral">—</span>';
    const cls = val >= 0 ? 'positive' : 'negative';
    let formatted;
    if (Math.abs(val) >= 1000) formatted = val.toLocaleString('en-US', { maximumFractionDigits: 0 });
    else if (Math.abs(val) >= 100) formatted = val.toFixed(0);
    else formatted = val.toFixed(2);
    return `<span class="${cls}">${formatted}%</span>`;
}

function formatPNL(val) {
    if (!val || val === 0) return '<span class="neutral">0</span>';
    const cls = val > 0 ? 'pnl-positive' : 'pnl-negative';
    const sign = val > 0 ? '+' : '';
    return `<span class="${cls}">${sign}${formatSOL(val)}</span>`;
}

function formatPNLBig(val) {
    if (!val || val === 0) return '<span class="neutral">0</span>';
    const cls = val > 0 ? 'positive' : 'negative';
    const sign = val > 0 ? '+' : '';
    return `<span class="pnl-big ${cls}">${sign}${formatSOL(val)} SOL</span>`;
}

function formatWinRate(rate) {
    const fillClass = rate >= 60 ? 'winrate-high' : (rate >= 40 ? 'winrate-mid' : 'winrate-low');
    return `<div class="winrate-container">
        <span>${rate.toFixed(1)}%</span>
        <div class="winrate-bar"><div class="winrate-fill ${fillClass}" style="width:${Math.min(rate, 100)}%"></div></div>
    </div>`;
}

function formatROI(val) {
    if (!val) return '<span class="neutral">0%</span>';
    const cls = val >= 0 ? 'positive' : 'negative';
    const sign = val >= 0 ? '+' : '';
    return `<span class="${cls}">${sign}${val.toFixed(1)}%</span>`;
}

function renderTags(tags) {
    if (!tags) return '';
    return tags.split(',').map(t => {
        const tag = t.trim().toLowerCase();
        let cls = tag;
        if (tag.includes('whale')) cls = 'tag-whale';
        else if (tag.includes('bot')) cls = 'tag-bot';
        else if (tag.includes('insider')) cls = 'tag-insider';
        return `<span class="${cls}">${tag}</span>`;
    }).join('');
}


// ═══════════════════════════════════════
// Classification Formatting
// ═══════════════════════════════════════

function formatInvestorBadge(classification) {
    if (!classification || !classification.investor_type || classification.investor_type === 'unclassified') {
        return '';
    }
    const c = classification;
    const typeConfig = {
        'elite':          { cls: 'inv-elite',       icon: '🟢' },
        'profitable':     { cls: 'inv-profitable',  icon: '🔵' },
        'regular':        { cls: 'inv-regular',     icon: '⚪' },
        'bot_profitable': { cls: 'inv-bot-profit',  icon: '🤖' },
        'bot_regular':    { cls: 'inv-bot',         icon: '🤖' },
        'casual':         { cls: 'inv-casual',      icon: '💤' },
        'losing':         { cls: 'inv-losing',      icon: '🔴' },
    };
    const cfg = typeConfig[c.investor_type] || { cls: '', icon: '❓' };
    let html = `<span class="tag ${cfg.cls}">${cfg.icon} ${c.investor_type.replace('_', ' ')}</span>`;
    if (c.investor_score > 0) {
        html += `<span class="inv-score">${c.investor_score}</span>`;
    }
    return html;
}

function formatInvestorScore(score) {
    if (!score || score === 0) return '<span class="neutral">-</span>';
    let cls = 'inv-score-low';
    if (score >= 70) cls = 'inv-score-high';
    else if (score >= 40) cls = 'inv-score-mid';
    return `<span class="inv-score-cell ${cls}">${score}</span>`;
}

function formatInvestors(investors) {
    if (!investors || !investors.total) {
        return '<span class="neutral">-</span>';
    }
    const inv = investors;
    let badges = [];
    if (inv.elite > 0) {
        badges.push(`<span class="inv-badge inv-elite" title="${inv.elite} Elite Traders">🟢${inv.elite}</span>`);
    }
    if (inv.profitable > 0) {
        badges.push(`<span class="inv-badge inv-profitable" title="${inv.profitable} Profitable Traders">🔵${inv.profitable}</span>`);
    }
    if (inv.regular > 0) {
        badges.push(`<span class="inv-badge inv-regular" title="${inv.regular} Regular Traders">⚪${inv.regular}</span>`);
    }

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


// ═══════════════════════════════════════
// Data Fetching
// ═══════════════════════════════════════

async function fetchData() {
    try {
        if (currentView === 'traders') {
            const url = `${API_BASE}/top-traders?sort=${currentSort}&order=${currentOrder}&limit=50&min_trades=3&time_range=${currentTimeRange}`;
            const res = await fetch(url);
            const data = await res.json();
            if (data.success) {
                allData = data.traders;
                renderTraders(allData);
                updateStatus(true, data.count, 'traders');
            } else {
                allData = [];
                renderTraders([]);
                updateStatus(false, 0, 'traders', data.error);
            }
        } else {
            const url = `${API_BASE}/tokens?sort=${currentSort}&order=${currentOrder}&limit=50`;
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
        const res = await fetch(`${API_BASE}/stats`);
        const data = await res.json();
        if (data.success) {
            document.getElementById('statTokens').textContent = data.total_tokens.toLocaleString() + ' tokens';
            document.getElementById('statWallets').textContent = data.total_wallets.toLocaleString() + ' wallets';
            document.getElementById('statTxns').textContent = data.transactions_24h.toLocaleString() + ' txns/24h';
            // Clasificaciones
            if (data.classifications && data.classifications.total > 0) {
                const c = data.classifications;
                const classEl = document.getElementById('statClassifications');
                if (classEl) {
                    classEl.textContent = `${c.total} classified (${c.humans}👤 ${c.bots}🤖 ${c.elite}🟢)`;
                }
            }
        }
    } catch (err) { /* silent */ }
}


// ═══════════════════════════════════════
// Render Top Traders
// ═══════════════════════════════════════

function renderTraders(traders) {
    const tbody = document.getElementById('tableBody');
    let filtered = traders;

    if (searchTerm) {
        const term = searchTerm.toLowerCase();
        filtered = traders.filter(t =>
            t.wallet_address.toLowerCase().includes(term) ||
            (t.tags && t.tags.toLowerCase().includes(term)) ||
            (t.classification && t.classification.investor_type && t.classification.investor_type.toLowerCase().includes(term))
        );
    }

    if (filtered.length === 0) {
        tbody.innerHTML = `<tr><td colspan="12" style="text-align:center;padding:40px;color:#5c5e64">${searchTerm ? 'No se encontraron traders' : 'No hay traders con suficientes trades'}</td></tr>`;
        return;
    }

    tbody.innerHTML = filtered.map((t, i) => {
        const rank = i + 1;
        const rankClass = rank === 1 ? 'gold' : (rank === 2 ? 'silver' : (rank === 3 ? 'bronze' : ''));
        const avatarClass = rank === 1 ? 'top1' : (rank === 2 ? 'top2' : (rank === 3 ? 'top3' : 'normal'));
        const rankIcon = rank === 1 ? '👑' : (rank === 2 ? '🥈' : (rank === 3 ? '🥉' : ''));
        const shortAddr = t.wallet_address.slice(0, 6) + '...' + t.wallet_address.slice(-4);

        const classificationBadge = formatInvestorBadge(t.classification);
        const score = t.classification ? t.classification.investor_score : 0;

        return `<tr onclick="window.open('https://solscan.io/account/${t.wallet_address}','_blank')" title="${t.wallet_address}">
            <td>
                <div class="trader-cell">
                    <span class="trader-rank ${rankClass}">${rank}</span>
                    <div class="trader-avatar ${avatarClass}">${rankIcon}</div>
                    <div class="trader-info">
                        <span class="trader-address">${shortAddr}</span>
                        <span class="trader-tags">${renderTags(t.tags)}${classificationBadge} ${t.tokens_traded} tokens tradeados</span>
                    </div>
                </div>
            </td>
            <td>${formatPNLBig(t.total_pnl)}</td>
            <td>${formatWinRate(t.win_rate)}</td>
            <td>${formatROI(t.roi_percentage)}</td>
            <td>${formatInvestorScore(score)}</td>
            <td>${formatNumber(t.total_trades)}</td>
            <td>${formatSOL(t.total_invested)} SOL</td>
            <td>${formatPNL(t.total_realized)}</td>
            <td>${formatPNL(t.best_trade)}</td>
            <td>${formatPNL(t.worst_trade)}</td>
            <td>${t.tokens_traded}</td>
            <td class="age">${t.last_activity}</td>
        </tr>`;
    }).join('');
}


// ═══════════════════════════════════════
// Render Tokens
// ═══════════════════════════════════════

function renderTokens(tokens) {
    const tbody = document.getElementById('tableBody');
    let filtered = tokens;

    if (searchTerm) {
        const term = searchTerm.toLowerCase();
        filtered = tokens.filter(t =>
            (t.name && t.name.toLowerCase().includes(term)) ||
            (t.symbol && t.symbol.toLowerCase().includes(term)) ||
            (t.mint_address && t.mint_address.toLowerCase().includes(term))
        );
    }

    if (filtered.length === 0) {
        tbody.innerHTML = `<tr><td colspan="13" style="text-align:center;padding:40px;color:#5c5e64">${searchTerm ? 'No se encontraron tokens' : 'No hay datos disponibles'}</td></tr>`;
        return;
    }

    tbody.innerHTML = filtered.map((t, i) => {
        const imgHTML = t.image_url
            ? `<img src="${t.image_url}" alt="" onerror="this.parentElement.innerHTML='🪙'">`
            : '🪙';
        const ammBadge = t.amm ? `<span class="badge badge-amm">${t.amm}</span>` : '';

        let displaySymbol = t.symbol || '???';
        let displayName = t.name || 'Unknown';

        return `<tr onclick="window.open('https://dexscreener.com/solana/${t.mint_address}','_blank')" title="${t.mint_address}">
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
            <td>${formatNumber(t.txns)}</td>
            <td>${formatCompact(t.volume_24h)}</td>
            <td>${formatNumber(t.makers)}</td>
            <td>${formatPercent(t.pct_5m)}</td>
            <td>${formatPercent(t.pct_1h)}</td>
            <td>${formatPercent(t.pct_6h)}</td>
            <td>${formatPercent(t.pct_24h)}</td>
            <td>${formatCompact(t.liquidity)}</td>
            <td>${formatCompact(t.market_cap)}</td>
        </tr>`;
    }).join('');
}


// ═══════════════════════════════════════
// UI: View switching & filters
// ═══════════════════════════════════════

function switchView(view) {
    currentView = view;
    const config = VIEW_CONFIG[view];
    currentSort = config.defaultSort;
    currentOrder = 'desc';
    currentTimeRange = 'all';
    searchTerm = '';
    document.getElementById('searchBox').value = '';
    allData = [];
    document.getElementById('tableBody').innerHTML = '<tr><td colspan="13" style="text-align:center;padding:40px;color:#5c5e64">Cargando datos...</td></tr>';

    // Update view buttons
    document.querySelectorAll('.view-btn').forEach(b => b.classList.remove('active'));
    document.querySelector(`.view-btn[data-view="${view}"]`).classList.add('active');

    // Update headers
    document.getElementById('tableHead').innerHTML = config.headers;

    // Build sub-filters
    const subfilters = document.getElementById('subfilters');
    let filtersHTML = config.filters.map((f, i) =>
        `<button class="filter-btn sort-filter ${i === 0 ? 'active' : ''}" data-sort="${f.sort}">${f.label}</button>`
    ).join('');

    if (config.timeFilters && config.timeFilters.length > 0) {
        filtersHTML += '<span style="border-left:1px solid #2a2d35;height:20px;margin:0 8px;align-self:center"></span>';
        filtersHTML += config.timeFilters.map(tf =>
            `<button class="filter-btn time-filter ${tf.value === 'all' ? 'active' : ''}" data-time="${tf.value}">${tf.label}</button>`
        ).join('');
    }

    subfilters.innerHTML = filtersHTML;

    // Sort filter click handlers
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

    // Time filter click handlers
    subfilters.querySelectorAll('.time-filter').forEach(btn => {
        btn.addEventListener('click', () => {
            subfilters.querySelectorAll('.time-filter').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            currentTimeRange = btn.dataset.time;
            fetchData();
        });
    });

    document.getElementById('searchBox').placeholder = view === 'traders'
        ? 'Buscar wallet, tag o tipo...'
        : 'Buscar token o pegar dirección...';

    fetchData();
}

function updateStatus(connected, count, type, error) {
    const statusEl = document.getElementById('connectionStatus');
    const updateEl = document.getElementById('lastUpdate');
    if (connected) {
        const label = type === 'traders' ? 'traders' : 'tokens';
        const timeLabel = (currentView === 'traders' && currentTimeRange !== 'all') ? ` (${currentTimeRange.toUpperCase()})` : '';
        statusEl.innerHTML = `<span class="status-dot connected"></span> Conectado — ${count} ${label}${timeLabel}`;
        updateEl.textContent = 'Última actualización: ' + new Date().toLocaleTimeString();
    } else {
        statusEl.innerHTML = `<span class="status-dot error"></span> Error: ${error || 'sin conexión'}`;
    }
}


// ═══════════════════════════════════════
// Event Listeners & Init
// ═══════════════════════════════════════

document.querySelectorAll('.view-btn').forEach(btn => {
    btn.addEventListener('click', () => switchView(btn.dataset.view));
});

document.getElementById('searchBox').addEventListener('input', (e) => {
    searchTerm = e.target.value;
    if (currentView === 'traders') renderTraders(allData);
    else renderTokens(allData);
});

// Init — Top Traders first
switchView('traders');
fetchStats();
setInterval(fetchData, REFRESH_INTERVAL);
setInterval(fetchStats, 60000);

console.log('🚀 Memecoin Screener v3 — Top Traders + Tokens + Investor Classification');
