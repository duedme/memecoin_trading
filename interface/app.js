// ══════════════════════════════════════════════════
// app.js — Frontend que consume la API de tu backend
// ══════════════════════════════════════════════════

const API_BASE = 'http://localhost:5000/api';
const REFRESH_INTERVAL = 15000; // Refrescar cada 15 segundos

let currentSort = 'volume';
let currentOrder = 'desc';
let searchTerm = '';
let allTokens = [];

// ── Formateo de números ──

function formatPrice(price) {
    if (!price || price === 0) return '$0';
    if (price < 0.00001) return '$' + price.toFixed(8);
    if (price < 0.001) return '$' + price.toFixed(6);
    if (price < 1) return '$' + price.toFixed(4);
    if (price < 1000) return '$' + price.toFixed(2);
    return '$' + price.toLocaleString('en-US', {maximumFractionDigits: 2});
}

function formatCompact(value) {
    if (!value || value === 0) return '$0';
    if (value >= 1e9) return '$' + (value / 1e9).toFixed(1) + 'B';
    if (value >= 1e6) return '$' + (value / 1e6).toFixed(1) + 'M';
    if (value >= 1e3) return '$' + (value / 1e3).toFixed(0) + 'K';
    return '$' + value.toFixed(0);
}

function formatNumber(num) {
    if (!num) return '0';
    return num.toLocaleString('en-US');
}

function formatPercent(val) {
    if (val === null || val === undefined) return '<span class="neutral">—</span>';
    const cls = val >= 0 ? 'positive' : 'negative';
    let formatted;
    if (Math.abs(val) >= 1000) formatted = val.toLocaleString('en-US', {maximumFractionDigits: 0}) + '%';
    else if (Math.abs(val) >= 100) formatted = val.toFixed(0) + '%';
    else formatted = val.toFixed(2) + '%';
    return `<span class="${cls}">${formatted}</span>`;
}

// ── Fetch datos de la API ──

async function fetchTokens() {
    try {
        const url = `${API_BASE}/tokens?sort=${currentSort}&order=${currentOrder}&limit=50`;
        const res = await fetch(url);
        const data = await res.json();

        if (data.success) {
            allTokens = data.tokens;
            renderTable(allTokens);
            updateStatus(true, data.count);
        } else {
            updateStatus(false, 0, data.error);
        }
    } catch (err) {
        console.error('Error fetching tokens:', err);
        updateStatus(false, 0, err.message);
    }
}

async function fetchStats() {
    try {
        const res = await fetch(`${API_BASE}/stats`);
        const data = await res.json();
        if (data.success) {
            document.getElementById('statTokens').textContent = `${data.total_tokens.toLocaleString()} tokens`;
            document.getElementById('statWallets').textContent = `${data.total_wallets.toLocaleString()} wallets`;
            document.getElementById('statTxns').textContent = `${data.transactions_24h.toLocaleString()} txns/24h`;
        }
    } catch (err) {
        console.error('Error fetching stats:', err);
    }
}

// ── Renderizar tabla ──

function renderTable(tokens) {
    const tbody = document.getElementById('tokenBody');

    // Filtrar por búsqueda
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
        tbody.innerHTML = `<tr><td colspan="12" style="text-align:center;padding:40px;color:#5c5e64;">
            ${searchTerm ? '🔍 No se encontraron tokens' : '📭 No hay datos disponibles'}
        </td></tr>`;
        return;
    }

    tbody.innerHTML = filtered.map((t, i) => {
        const imgHTML = t.image_url
            ? `<img src="${t.image_url}" alt="" onerror="this.parentElement.innerHTML='🪙'">`
            : '🪙';

        const ammBadge = t.amm
            ? `<span class="badge badge-amm">${t.amm.replace('auto-discovered','🔍')}</span>`
            : '';

        return `
        <tr onclick="window.open('https://dexscreener.com/solana/${t.mint_address}','_blank')" title="${t.mint_address}">
            <td>
                <div class="token-cell">
                    <span class="token-rank">#${i + 1}</span>
                    <div class="token-icon">${imgHTML}</div>
                    <div class="token-info">
                        <span class="token-name">
                            ${t.symbol || '???'}
                            <span class="token-badges">${ammBadge}</span>
                        </span>
                        <span class="token-chain">${t.name || 'Unknown'}</span>
                    </div>
                </div>
            </td>
            <td class="price">${formatPrice(t.price)}</td>
            <td class="age">${t.age}</td>
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

// ── Status ──

function updateStatus(connected, count, error) {
    const dot = document.querySelector('.status-dot');
    const statusEl = document.getElementById('connectionStatus');
    const updateEl = document.getElementById('lastUpdate');

    if (connected) {
        dot.className = 'status-dot connected';
        statusEl.innerHTML = `<span class="status-dot connected"></span> Conectado · ${count} tokens`;
        updateEl.textContent = `Última actualización: ${new Date().toLocaleTimeString()}`;
    } else {
        dot.className = 'status-dot error';
        statusEl.innerHTML = `<span class="status-dot error"></span> Error: ${error || 'sin conexión'}`;
    }
}

// ── Event Listeners ──

// Filtros de ordenamiento
document.querySelectorAll('.filter-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        const newSort = btn.dataset.sort;
        if (newSort === currentSort) {
            currentOrder = currentOrder === 'desc' ? 'asc' : 'desc';
        } else {
            currentSort = newSort;
            currentOrder = newSort === 'age' ? 'asc' : 'desc';
        }
        fetchTokens();
    });
});

// Búsqueda
document.getElementById('searchBox').addEventListener('input', (e) => {
    searchTerm = e.target.value;
    renderTable(allTokens);
});

// ── Inicio ──

fetchTokens();
fetchStats();

// Auto-refresh
setInterval(fetchTokens, REFRESH_INTERVAL);
setInterval(fetchStats, 60000);

console.log('🦎 Memecoin Screener cargado');
console.log(`📡 API: ${API_BASE}`);
console.log(`🔄 Auto-refresh: cada ${REFRESH_INTERVAL/1000}s`);
