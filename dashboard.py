#!/usr/bin/env python3
"""
Nellis Auction Analytics Dashboard & Deal Sniper
A lightweight, zero-dependency local web app powered by Python & DuckDB with live countdowns.
"""

import http.server
import json
import os
import re
import socketserver
import sqlite3
import sys
import time
import urllib.parse
import webbrowser
from threading import Thread

import duckdb

PORT = 8501
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQLITE_DB = os.path.join(BASE_DIR, "nellis_auctions.db")

R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_ENDPOINT_URL = os.environ.get("R2_ENDPOINT_URL", "https://965a5411101d1521c551d99d9421a145.r2.cloudflarestorage.com")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME", "nellis-auction-lake")


def get_r2_duckdb_conn():
    """Create DuckDB connection configured with Cloudflare R2."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    host = R2_ENDPOINT_URL.replace("https://", "").replace("http://", "").strip("/")
    con.execute(f"""
        SET s3_endpoint = '{host}';
        SET s3_access_key_id = '{R2_ACCESS_KEY_ID}';
        SET s3_secret_access_key = '{R2_SECRET_ACCESS_KEY}';
        SET s3_url_style = 'path';
        SET s3_region = 'auto';
        SET s3_use_ssl = true;
    """)
    con.execute(f"CREATE OR REPLACE VIEW fact_auctions AS SELECT * FROM read_parquet('s3://{R2_BUCKET_NAME}/fact_auctions/*/*/*.parquet');")
    con.execute(f"CREATE OR REPLACE VIEW dim_products AS SELECT * FROM read_parquet('s3://{R2_BUCKET_NAME}/dim_products/products_catalog.parquet');")
    return con


_R2_CONN = None

def get_cached_r2_conn():
    global _R2_CONN
    if _R2_CONN is None:
        try:
            _R2_CONN = get_r2_duckdb_conn()
        except Exception as e:
            print(f"[!] Warning connecting to R2 DuckDB: {e}")
            return None
    return _R2_CONN


HTML_DASHBOARD = """<!DOCTYPE html>
<html lang="en" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Nellis Auction Sniper & Deal Finder</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script>
        tailwind.config = {
            darkMode: 'class',
            theme: {
                extend: {
                    colors: {
                        brand: { 50: '#f0fdf4', 500: '#22c55e', 600: '#16a34a', 700: '#15803d' },
                        darkbg: '#0f172a',
                        cardbg: '#1e293b',
                        bordercolor: '#334155'
                    }
                }
            }
        }
    </script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: #0f172a; }
        ::-webkit-scrollbar-thumb { background: #334155; border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: #475569; }
        @keyframes pulse-urgent {
            0%, 100% { opacity: 1; transform: scale(1); }
            50% { opacity: 0.85; transform: scale(1.03); }
        }
        .urgent-pulse {
            animation: pulse-urgent 1.5s infinite;
        }
    </style>
</head>
<body class="bg-darkbg text-slate-100 min-h-screen flex flex-col font-sans antialiased">

    <!-- Top Navigation Header -->
    <header class="border-b border-bordercolor bg-cardbg/80 backdrop-blur sticky top-0 z-50">
        <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
            <div class="flex items-center space-x-3">
                <div class="w-10 h-10 rounded-xl bg-gradient-to-tr from-rose-600 via-amber-500 to-emerald-400 flex items-center justify-center shadow-lg shadow-rose-500/20">
                    <i class="fa-solid fa-stopwatch text-white text-lg"></i>
                </div>
                <div>
                    <h1 class="text-lg font-bold tracking-tight bg-gradient-to-r from-white to-slate-300 bg-clip-text text-transparent">Nellis Auction Sniper</h1>
                    <p class="text-xs text-slate-400">Phoenix & Mesa, AZ Live Analytics</p>
                </div>
            </div>

            <!-- Navigation Tabs -->
            <nav class="flex space-x-1 bg-slate-900/60 p-1 rounded-xl border border-bordercolor">
                <button onclick="switchTab('deals')" id="tab-deals" class="px-4 py-1.5 rounded-lg text-sm font-medium transition-all bg-brand-600 text-white shadow">
                    <i class="fa-solid fa-crosshairs mr-1.5 text-amber-300"></i> Sniper & Deals
                </button>
                <button onclick="switchTab('analytics')" id="tab-analytics" class="px-4 py-1.5 rounded-lg text-sm font-medium text-slate-400 hover:text-white transition-all">
                    <i class="fa-solid fa-chart-pie mr-1.5 text-indigo-400"></i> Analytics Models
                </button>
                <button onclick="switchTab('sql')" id="tab-sql" class="px-4 py-1.5 rounded-lg text-sm font-medium text-slate-400 hover:text-white transition-all">
                    <i class="fa-solid fa-terminal mr-1.5 text-emerald-400"></i> SQL Studio
                </button>
            </nav>

            <!-- Quick Sniper Mode Preset Button -->
            <button onclick="quickFilterSniper()" class="hidden sm:flex items-center px-3 py-1.5 rounded-xl bg-rose-950/80 hover:bg-rose-900 border border-rose-800 text-rose-300 text-xs font-bold transition shadow-lg shadow-rose-900/20">
                <i class="fa-solid fa-fire mr-1.5 text-amber-400 animate-bounce"></i> Ending in &lt; 5m
            </button>
        </div>
    </header>

    <!-- Main Container -->
    <main class="flex-1 max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 w-full">

        <!-- Top KPI Metrics Banner -->
        <div id="stats-banner" class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
            <div class="bg-cardbg border border-bordercolor p-4 rounded-xl">
                <div class="text-xs font-medium text-slate-400 uppercase tracking-wider">Total Active Lots</div>
                <div id="stat-total-items" class="text-2xl font-bold mt-1 text-white">Loading...</div>
                <div class="text-xs text-slate-500 mt-0.5">Phoenix & Mesa Combined</div>
            </div>
            <div class="bg-cardbg border border-bordercolor p-4 rounded-xl">
                <div class="text-xs font-medium text-slate-400 uppercase tracking-wider">Catalog Retail Value</div>
                <div id="stat-total-retail" class="text-2xl font-bold mt-1 text-emerald-400">Loading...</div>
                <div class="text-xs text-slate-500 mt-0.5">MSRP Total</div>
            </div>
            <div class="bg-cardbg border border-bordercolor p-4 rounded-xl">
                <div class="text-xs font-medium text-slate-400 uppercase tracking-wider">Average Discount</div>
                <div id="stat-avg-discount" class="text-2xl font-bold mt-1 text-amber-400">Loading...</div>
                <div class="text-xs text-slate-500 mt-0.5">Current vs Retail</div>
            </div>
            <div class="bg-cardbg border border-bordercolor p-4 rounded-xl">
                <div class="text-xs font-medium text-slate-400 uppercase tracking-wider">Brand New Lots</div>
                <div id="stat-new-items" class="text-2xl font-bold mt-1 text-sky-400">Loading...</div>
                <div class="text-xs text-slate-500 mt-0.5">Factory Sealed & New</div>
            </div>
        </div>

        <!-- TAB 1: DEAL FINDER & SNIPER BROWSER -->
        <section id="view-deals" class="space-y-6">
            <!-- Filter Controls -->
            <div class="bg-cardbg border border-bordercolor p-4 rounded-2xl space-y-4">
                <div class="flex flex-col md:flex-row gap-3">
                    <div class="flex-1 relative">
                        <i class="fa-solid fa-magnifying-glass absolute left-3.5 top-3.5 text-slate-400"></i>
                        <input type="text" id="filter-search" placeholder="Search keywords (e.g. DeWalt, Dyson, Lego, Monitor, Tent, Air Conditioner)..."
                               class="w-full bg-slate-900 border border-bordercolor rounded-xl pl-10 pr-4 py-2.5 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-brand-500 transition">
                    </div>
                    <button onclick="triggerSearch()" class="px-5 py-2.5 bg-brand-600 hover:bg-brand-500 text-white text-sm font-semibold rounded-xl shadow-lg shadow-brand-600/20 transition flex items-center justify-center">
                        <i class="fa-solid fa-filter mr-2"></i> Apply Filters
                    </button>
                    <button onclick="resetFilters()" class="px-4 py-2.5 bg-slate-800 hover:bg-slate-700 text-slate-300 text-sm font-medium rounded-xl transition">
                        Reset
                    </button>
                </div>

                <!-- Dropdown / Pill Filters -->
                <div class="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-6 gap-3 pt-2 border-t border-bordercolor/60 text-xs">
                    <!-- NEW TIME REMAINING FILTER -->
                    <div class="col-span-2 sm:col-span-1">
                        <label class="block text-amber-400 font-bold mb-1 flex items-center">
                            <i class="fa-solid fa-stopwatch mr-1"></i> Time Remaining
                        </label>
                        <select id="filter-time-remaining" onchange="triggerSearch()" class="w-full bg-slate-900 border border-amber-500/50 rounded-lg px-2.5 py-1.5 text-amber-300 font-semibold focus:outline-none focus:border-amber-400">
                            <option value="all">Any Time Remaining</option>
                            <option value="5m">🔥 Ending in &lt; 5 mins</option>
                            <option value="15m">⚡ Ending in &lt; 15 mins</option>
                            <option value="30m">⏳ Ending in &lt; 30 mins</option>
                            <option value="1h">⏱️ Ending in &lt; 1 hour</option>
                            <option value="3h">Ending in &lt; 3 hours</option>
                            <option value="today">Ending Today</option>
                        </select>
                    </div>

                    <div>
                        <label class="block text-slate-400 font-medium mb-1">Location</label>
                        <select id="filter-location" class="w-full bg-slate-900 border border-bordercolor rounded-lg px-2.5 py-1.5 text-slate-200">
                            <option value="all">Phoenix & Mesa</option>
                            <option value="Phoenix">Phoenix Only</option>
                            <option value="Mesa">Mesa Only</option>
                        </select>
                    </div>
                    <div>
                        <label class="block text-slate-400 font-medium mb-1">Condition</label>
                        <select id="filter-condition" class="w-full bg-slate-900 border border-bordercolor rounded-lg px-2.5 py-1.5 text-slate-200">
                            <option value="all">Any Condition</option>
                            <option value="New">Brand New Only</option>
                            <option value="Used">Used</option>
                            <option value="Damaged">Damaged / Salvage</option>
                        </select>
                    </div>
                    <div>
                        <label class="block text-slate-400 font-medium mb-1">Min Retail ($)</label>
                        <input type="number" id="filter-min-retail" placeholder="e.g. 50" min="0" class="w-full bg-slate-900 border border-bordercolor rounded-lg px-2.5 py-1.5 text-slate-200">
                    </div>
                    <div>
                        <label class="block text-slate-400 font-medium mb-1">Max Bid ($)</label>
                        <input type="number" id="filter-max-bid" placeholder="e.g. 40" min="0" class="w-full bg-slate-900 border border-bordercolor rounded-lg px-2.5 py-1.5 text-slate-200">
                    </div>
                    <div>
                        <label class="block text-slate-400 font-medium mb-1">Sort Results By</label>
                        <select id="filter-sort" class="w-full bg-slate-900 border border-bordercolor rounded-lg px-2.5 py-1.5 text-slate-200">
                            <option value="closing_soon">⏳ Closing Soonest (Sniper)</option>
                            <option value="discount_desc">Highest Discount %</option>
                            <option value="retail_desc">Highest Retail Value</option>
                            <option value="bid_asc">Lowest Current Bid</option>
                        </select>
                    </div>
                </div>
            </div>

            <!-- Results Count & Live Status -->
            <div class="flex items-center justify-between text-xs text-slate-400 px-1">
                <span id="results-count" class="font-medium text-slate-300">Loading auction items...</span>
                <span class="flex items-center text-emerald-400 font-semibold">
                    <span class="w-2 h-2 rounded-full bg-emerald-400 animate-ping mr-1.5"></span> Live countdown active
                </span>
            </div>

            <!-- Items Cards Grid -->
            <div id="items-grid" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                <!-- Dynamically rendered lot cards -->
            </div>

            <!-- Pagination Bar -->
            <div class="flex items-center justify-center space-x-2 pt-4">
                <button onclick="prevPage()" id="btn-prev" class="px-3 py-1.5 rounded-lg bg-cardbg border border-bordercolor text-slate-300 disabled:opacity-40">Previous</button>
                <span id="page-indicator" class="text-xs text-slate-400 px-2">Page 1</span>
                <button onclick="nextPage()" id="btn-next" class="px-3 py-1.5 rounded-lg bg-cardbg border border-bordercolor text-slate-300">Next</button>
            </div>
        </section>

        <!-- TAB 2: ANALYTICAL MODELS -->
        <section id="view-analytics" class="hidden space-y-6">
            <div class="flex flex-wrap gap-2">
                <button onclick="runAnalyticsModel('category-spread')" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 rounded-xl text-xs font-semibold text-white transition">
                    <i class="fa-solid fa-tags mr-1.5"></i> Category Value Spreads
                </button>
                <button onclick="runAnalyticsModel('condition-arbitrage')" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 rounded-xl text-xs font-semibold text-white transition">
                    <i class="fa-solid fa-scale-balanced mr-1.5"></i> Condition Risk Arbitrage
                </button>
                <button onclick="runAnalyticsModel('temporal')" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 rounded-xl text-xs font-semibold text-white transition">
                    <i class="fa-solid fa-clock mr-1.5"></i> Temporal Sweet Spots (MST)
                </button>
                <button onclick="runAnalyticsModel('sleepers')" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 rounded-xl text-xs font-semibold text-white transition">
                    <i class="fa-solid fa-gem mr-1.5"></i> Sleeper Lot Hunter
                </button>
            </div>

            <div class="bg-cardbg border border-bordercolor p-5 rounded-2xl">
                <div class="flex items-center justify-between mb-4">
                    <div>
                        <h2 id="model-title" class="text-base font-bold text-white">Select an Analytical Model</h2>
                        <p id="model-subtitle" class="text-xs text-slate-400">Run sub-second analytical queries against the Parquet Data Lake.</p>
                    </div>
                    <button onclick="exportTableToCSV('analytics-table')" class="text-xs bg-slate-800 hover:bg-slate-700 text-slate-300 px-3 py-1.5 rounded-lg border border-bordercolor transition">
                        <i class="fa-solid fa-download mr-1"></i> Export CSV
                    </button>
                </div>
                <div class="overflow-x-auto max-h-[600px] overflow-y-auto">
                    <table id="analytics-table" class="w-full text-left text-xs border-collapse">
                        <!-- Dynamically filled table -->
                    </table>
                </div>
            </div>
        </section>

        <!-- TAB 3: SQL STUDIO -->
        <section id="view-sql" class="hidden space-y-6">
            <div class="bg-cardbg border border-bordercolor p-5 rounded-2xl space-y-3">
                <div class="flex items-center justify-between">
                    <div class="text-sm font-bold text-white flex items-center">
                        <i class="fa-solid fa-database mr-2 text-brand-500"></i> DuckDB Cloud SQL Console
                    </div>
                    <div class="text-xs text-slate-400">
                        Available tables: <span class="text-emerald-400 font-mono">fact_auctions</span>, <span class="text-indigo-400 font-mono">dim_products</span>
                    </div>
                </div>
                <textarea id="sql-input" rows="4" class="w-full bg-slate-900 border border-bordercolor rounded-xl p-3 text-xs font-mono text-emerald-300 focus:outline-none focus:border-brand-500 transition" placeholder="SELECT * FROM fact_auctions LIMIT 10;">SELECT 
    f.location,
    f.condition,
    COUNT(*) as total_lots,
    ROUND(AVG(f.retail_price), 2) as avg_retail,
    ROUND(AVG(f.final_price), 2) as avg_final_bid,
    ROUND(AVG(f.discount_ratio), 3) as avg_discount_ratio
FROM fact_auctions f
GROUP BY f.location, f.condition
ORDER BY total_lots DESC;</textarea>
                <div class="flex items-center justify-between pt-1">
                    <button onclick="executeCustomSQL()" class="px-5 py-2 bg-brand-600 hover:bg-brand-500 text-white text-xs font-semibold rounded-xl transition flex items-center">
                        <i class="fa-solid fa-play mr-1.5"></i> Run Query
                    </button>
                    <button onclick="exportTableToCSV('sql-table')" class="text-xs bg-slate-800 hover:bg-slate-700 text-slate-300 px-3 py-1.5 rounded-lg border border-bordercolor transition">
                        <i class="fa-solid fa-download mr-1"></i> Export Result to CSV
                    </button>
                </div>
            </div>

            <div class="bg-cardbg border border-bordercolor p-5 rounded-2xl">
                <div class="text-xs font-semibold text-slate-400 mb-2" id="sql-status">Ready to execute.</div>
                <div class="overflow-x-auto max-h-[600px] overflow-y-auto">
                    <table id="sql-table" class="w-full text-left text-xs border-collapse">
                        <!-- Dynamic SQL Table -->
                    </table>
                </div>
            </div>
        </section>

    </main>

    <script>
        let currentPage = 1;
        let activeLots = [];

        function switchTab(tab) {
            ['deals', 'analytics', 'sql'].forEach(t => {
                document.getElementById(`view-${t}`).classList.add('hidden');
                document.getElementById(`tab-${t}`).className = 'px-4 py-1.5 rounded-lg text-sm font-medium text-slate-400 hover:text-white transition-all';
            });
            document.getElementById(`view-${tab}`).classList.remove('hidden');
            document.getElementById(`tab-${tab}`).className = 'px-4 py-1.5 rounded-lg text-sm font-medium bg-brand-600 text-white shadow';

            if (tab === 'analytics' && !document.getElementById('analytics-table').innerHTML) {
                runAnalyticsModel('category-spread');
            }
        }

        async function loadStats() {
            try {
                const res = await fetch('/api/stats');
                const data = await res.json();
                document.getElementById('stat-total-items').innerText = Number(data.total_items).toLocaleString();
                document.getElementById('stat-total-retail').innerText = '$' + Number(data.total_retail).toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0});
                document.getElementById('stat-avg-discount').innerText = (data.avg_discount * 100).toFixed(1) + '%';
                document.getElementById('stat-new-items').innerText = Number(data.new_items).toLocaleString();
            } catch (e) {
                console.error(e);
            }
        }

        function formatCountdown(closeMs) {
            const now = Date.now();
            const diff = closeMs - now;
            if (diff <= 0) {
                return { text: "Closed", badgeClass: "bg-slate-800 text-slate-400 border-slate-700", isUrgent: false };
            }
            const totalSecs = Math.floor(diff / 1000);
            const totalMins = Math.floor(totalSecs / 60);
            const secs = totalSecs % 60;
            const hours = Math.floor(totalMins / 60);
            const mins = totalMins % 60;

            if (totalMins < 5) {
                return {
                    text: `🔥 ${totalMins}m ${secs.toString().padStart(2, '0')}s left`,
                    badgeClass: "bg-rose-950 text-rose-300 border-rose-500 font-black urgent-pulse",
                    isUrgent: true
                };
            } else if (totalMins < 15) {
                return {
                    text: `⚡ ${totalMins}m ${secs.toString().padStart(2, '0')}s left`,
                    badgeClass: "bg-amber-950 text-amber-300 border-amber-600 font-bold",
                    isUrgent: true
                };
            } else if (hours < 1) {
                return {
                    text: `⏱️ ${totalMins}m left`,
                    badgeClass: "bg-sky-950 text-sky-300 border-sky-700 font-semibold",
                    isUrgent: false
                };
            } else if (hours < 24) {
                return {
                    text: `⏱️ ${hours}h ${mins}m left`,
                    badgeClass: "bg-slate-900 text-slate-300 border-slate-700",
                    isUrgent: false
                };
            } else {
                const days = Math.floor(hours / 24);
                return {
                    text: `📅 ${days}d ${hours % 24}h left`,
                    badgeClass: "bg-slate-900 text-slate-400 border-slate-800",
                    isUrgent: false
                };
            }
        }

        // Live Countdown Tick Function (Updates DOM every second without re-rendering)
        function updateLiveCountdowns() {
            document.querySelectorAll('[data-close-ms]').forEach(el => {
                const closeMs = Number(el.getAttribute('data-close-ms'));
                const { text, badgeClass } = formatCountdown(closeMs);
                el.innerText = text;
                el.className = `px-2 py-0.5 rounded-lg text-[11px] border backdrop-blur ${badgeClass}`;
            });
        }
        setInterval(updateLiveCountdowns, 1000);

        async function fetchItems() {
            const search = document.getElementById('filter-search').value;
            const location = document.getElementById('filter-location').value;
            const condition = document.getElementById('filter-condition').value;
            const timeRemaining = document.getElementById('filter-time-remaining').value;
            const minRetail = document.getElementById('filter-min-retail').value;
            const maxBid = document.getElementById('filter-max-bid').value;
            const sort = document.getElementById('filter-sort').value;

            const params = new URLSearchParams({
                search, location, condition, timeRemaining, minRetail, maxBid, sort, page: currentPage, limit: 18
            });

            const grid = document.getElementById('items-grid');
            grid.innerHTML = '<div class="col-span-full py-12 text-center text-slate-400"><i class="fa-solid fa-spinner fa-spin text-2xl mb-2 text-brand-500"></i><br>Querying auction lots...</div>';

            try {
                const res = await fetch(`/api/items?${params.toString()}`);
                const data = await res.json();
                activeLots = data.items || [];

                document.getElementById('results-count').innerText = `Showing ${activeLots.length} of ${data.total.toLocaleString()} matching lots`;
                document.getElementById('page-indicator').innerText = `Page ${currentPage} of ${Math.ceil(data.total / 18) || 1}`;
                document.getElementById('btn-prev').disabled = currentPage <= 1;
                document.getElementById('btn-next').disabled = currentPage >= Math.ceil(data.total / 18);

                if (!activeLots.length) {
                    grid.innerHTML = '<div class="col-span-full py-12 text-center text-slate-400">No auction items found matching your filters.</div>';
                    return;
                }

                grid.innerHTML = activeLots.map(item => {
                    const discount = item.discount_pct ? `${Math.round(item.discount_pct)}% OFF` : 'N/A';
                    const isNew = item.condition_type === 'New';
                    const condClass = isNew ? 'bg-emerald-950 text-emerald-400 border-emerald-800' : 'bg-amber-950 text-amber-400 border-amber-800';
                    const imgUrl = item.photo_url || 'https://images.unsplash.com/photo-1586769852044-692d6e3703f0?w=300&h=300&fit=crop';
                    const { text: timeText, badgeClass: timeBadgeClass } = formatCountdown(item.close_time_ms);

                    return `
                    <div class="bg-cardbg border border-bordercolor rounded-2xl overflow-hidden hover:border-slate-500 transition-all flex flex-col group shadow-md hover:shadow-xl">
                        <div class="h-48 w-full bg-slate-900 relative overflow-hidden flex items-center justify-center p-2">
                            <img src="${imgUrl}" alt="${item.title}" class="max-h-full max-w-full object-contain group-hover:scale-105 transition duration-300">
                            
                            <!-- Condition Tag -->
                            <span class="absolute top-2.5 left-2.5 px-2 py-0.5 rounded-full text-[11px] font-bold border ${condClass}">
                                ${item.condition_type || 'Used'}
                            </span>

                            <!-- Time Remaining Countdown Pill -->
                            <span data-close-ms="${item.close_time_ms}" class="absolute top-2.5 right-2.5 px-2 py-0.5 rounded-lg text-[11px] border backdrop-blur ${timeBadgeClass}">
                                ${timeText}
                            </span>

                            <!-- Discount Tag -->
                            <span class="absolute bottom-2.5 right-2.5 px-2 py-0.5 rounded-lg text-xs font-black bg-rose-600 text-white shadow">
                                ${discount}
                            </span>

                            <!-- Warehouse Tag -->
                            <span class="absolute bottom-2.5 left-2.5 px-2 py-0.5 rounded-lg text-[10px] font-semibold bg-slate-900/80 text-slate-300 border border-bordercolor backdrop-blur">
                                <i class="fa-solid fa-location-dot text-brand-500 mr-1"></i>${item.location_name}
                            </span>
                        </div>
                        <div class="p-4 flex-1 flex flex-col justify-between space-y-3">
                            <div>
                                <h3 class="text-xs font-semibold text-white line-clamp-2" title="${item.title}">${item.title}</h3>
                                ${item.inspector_notes ? `<p class="text-[11px] text-amber-300/90 line-clamp-1 mt-1 bg-amber-950/40 px-2 py-0.5 rounded border border-amber-800/40"><i class="fa-solid fa-triangle-exclamation mr-1"></i>${item.inspector_notes}</p>` : ''}
                            </div>
                            <div class="pt-2 border-t border-bordercolor/60 flex items-center justify-between">
                                <div>
                                    <div class="text-[10px] text-slate-400">Winning / Current Bid</div>
                                    <div class="text-base font-black text-white">$${item.winning_bid_amount.toFixed(2)}</div>
                                </div>
                                <div class="text-right">
                                    <div class="text-[10px] text-slate-400">Est. Retail</div>
                                    <div class="text-xs font-bold text-slate-400 line-through">$${item.estimated_retail_price.toFixed(2)}</div>
                                </div>
                            </div>
                            <a href="${item.item_url}" target="_blank" class="w-full text-center py-2 bg-slate-900 hover:bg-brand-600 border border-bordercolor hover:border-brand-500 text-xs font-semibold rounded-xl text-white transition flex items-center justify-center">
                                View Lot on Nellis <i class="fa-solid fa-arrow-up-right-from-square ml-1.5 text-[10px]"></i>
                            </a>
                        </div>
                    </div>
                    `;
                }).join('');

            } catch (e) {
                console.error(e);
                grid.innerHTML = '<div class="col-span-full py-12 text-center text-rose-400">Error fetching items from database.</div>';
            }
        }

        function quickFilterSniper() {
            document.getElementById('filter-time-remaining').value = '5m';
            document.getElementById('filter-sort').value = 'closing_soon';
            switchTab('deals');
            triggerSearch();
        }

        function triggerSearch() {
            currentPage = 1;
            fetchItems();
        }

        function resetFilters() {
            document.getElementById('filter-search').value = '';
            document.getElementById('filter-location').value = 'all';
            document.getElementById('filter-condition').value = 'all';
            document.getElementById('filter-time-remaining').value = 'all';
            document.getElementById('filter-min-retail').value = '';
            document.getElementById('filter-max-bid').value = '';
            document.getElementById('filter-sort').value = 'closing_soon';
            currentPage = 1;
            fetchItems();
        }

        function prevPage() {
            if (currentPage > 1) {
                currentPage--;
                fetchItems();
            }
        }

        function nextPage() {
            currentPage++;
            fetchItems();
        }

        async function runAnalyticsModel(model) {
            const table = document.getElementById('analytics-table');
            table.innerHTML = '<tr><td class="p-8 text-center text-slate-400"><i class="fa-solid fa-spinner fa-spin mr-2"></i>Executing analytical query over Cloudflare R2...</td></tr>';
            try {
                const res = await fetch(`/api/analytics?model=${model}`);
                const data = await res.json();
                document.getElementById('model-title').innerText = data.title;
                document.getElementById('model-subtitle').innerText = data.subtitle;
                renderTable(table, data.columns, data.rows);
            } catch (e) {
                table.innerHTML = '<tr><td class="p-8 text-center text-rose-400">Error executing model.</td></tr>';
            }
        }

        async function executeCustomSQL() {
            const sql = document.getElementById('sql-input').value;
            const table = document.getElementById('sql-table');
            const status = document.getElementById('sql-status');
            status.innerText = 'Executing query over Cloudflare R2 Data Lake...';
            table.innerHTML = '<tr><td class="p-8 text-center text-slate-400"><i class="fa-solid fa-spinner fa-spin mr-2"></i>Processing DuckDB query...</td></tr>';

            try {
                const res = await fetch('/api/sql', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ sql })
                });
                const data = await res.json();
                if (data.error) {
                    status.innerText = 'Error: ' + data.error;
                    table.innerHTML = `<tr><td class="p-4 text-rose-400 font-mono text-xs">${data.error}</td></tr>`;
                    return;
                }
                status.innerText = `Query executed successfully: ${data.rows.length} rows returned.`;
                renderTable(table, data.columns, data.rows);
            } catch (e) {
                status.innerText = 'Execution error: ' + e;
                table.innerHTML = '<tr><td class="p-4 text-rose-400">Failed to connect to backend server.</td></tr>';
            }
        }

        function renderTable(tableEl, cols, rows) {
            if (!rows || !rows.length) {
                tableEl.innerHTML = '<tr><td class="p-4 text-slate-400">No records returned.</td></tr>';
                return;
            }
            const thead = `<thead><tr class="bg-slate-900 border-b border-bordercolor">${cols.map(c => `<th class="p-2.5 font-bold text-slate-300 uppercase tracking-wider text-[11px]">${c}</th>`).join('')}</tr></thead>`;
            const tbody = `<tbody>${rows.map((r, idx) => `<tr class="border-b border-bordercolor/60 hover:bg-slate-800/40 ${idx % 2 === 0 ? 'bg-slate-900/20' : ''}">${r.map(val => `<td class="p-2.5 text-slate-300">${val !== null ? val : '<span class="text-slate-600">NULL</span>'}</td>`).join('')}</tr>`).join('')}</tbody>`;
            tableEl.innerHTML = thead + tbody;
        }

        function exportTableToCSV(tableId) {
            const table = document.getElementById(tableId);
            let csv = [];
            for (let row of table.rows) {
                let rowData = [];
                for (let cell of row.cells) {
                    rowData.push('"' + (cell.innerText || '').replace(/"/g, '""') + '"');
                }
                csv.push(rowData.join(','));
            }
            const blob = new Blob([csv.join('\\n')], { type: 'text/csv' });
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `${tableId}_export.csv`;
            a.click();
        }

        window.onload = () => {
            loadStats();
            fetchItems();
        };
    </script>
</body>
</html>
"""


class DashboardRequestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        params = urllib.parse.parse_qs(parsed.query)

        if path == "/" or path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_DASHBOARD.encode("utf-8"))
            return

        elif path == "/api/stats":
            self.handle_api_stats()
            return

        elif path == "/api/items":
            self.handle_api_items(params)
            return

        elif path == "/api/analytics":
            model = params.get("model", ["category-spread"])[0]
            self.handle_api_analytics(model)
            return

        else:
            self.send_error(404, "Not Found")

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/sql":
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8")
            try:
                data = json.loads(body)
                sql = data.get("sql", "")
                self.handle_api_sql(sql)
            except Exception as e:
                self.send_json({"error": str(e)}, status=400)
        else:
            self.send_error(404, "Not Found")

    def send_json(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode("utf-8"))

    def handle_api_stats(self):
        conn = sqlite3.connect(SQLITE_DB)
        cursor = conn.cursor()
        cursor.execute("SELECT count(*), COALESCE(SUM(estimated_retail_price), 0), COALESCE(AVG(discount_pct) / 100.0, 0) FROM auction_items")
        row = cursor.fetchone()
        cursor.execute("SELECT count(*) FROM auction_items WHERE condition_type = 'New'")
        new_count = cursor.fetchone()[0]
        conn.close()

        self.send_json({
            "total_items": row[0],
            "total_retail": row[1],
            "avg_discount": row[2],
            "new_items": new_count
        })

    def handle_api_items(self, params):
        search = params.get("search", [""])[0].strip()
        location = params.get("location", ["all"])[0].strip()
        condition = params.get("condition", ["all"])[0].strip()
        time_remaining = params.get("timeRemaining", ["all"])[0].strip()
        min_retail = params.get("minRetail", [""])[0].strip()
        max_bid = params.get("maxBid", [""])[0].strip()
        sort = params.get("sort", ["closing_soon"])[0].strip()
        page = int(params.get("page", ["1"])[0])
        limit = int(params.get("limit", ["18"])[0])
        offset = (page - 1) * limit

        conn = sqlite3.connect(SQLITE_DB)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        where_clauses = []
        sql_params = []

        now_ms = int(time.time() * 1000)

        # Time remaining filter logic
        if time_remaining == "5m":
            where_clauses.append("close_time_ms >= ? AND close_time_ms <= ?")
            sql_params.extend([now_ms, now_ms + (5 * 60 * 1000)])
        elif time_remaining == "15m":
            where_clauses.append("close_time_ms >= ? AND close_time_ms <= ?")
            sql_params.extend([now_ms, now_ms + (15 * 60 * 1000)])
        elif time_remaining == "30m":
            where_clauses.append("close_time_ms >= ? AND close_time_ms <= ?")
            sql_params.extend([now_ms, now_ms + (30 * 60 * 1000)])
        elif time_remaining == "1h":
            where_clauses.append("close_time_ms >= ? AND close_time_ms <= ?")
            sql_params.extend([now_ms, now_ms + (60 * 60 * 1000)])
        elif time_remaining == "3h":
            where_clauses.append("close_time_ms >= ? AND close_time_ms <= ?")
            sql_params.extend([now_ms, now_ms + (3 * 3600 * 1000)])
        elif time_remaining == "today":
            where_clauses.append("close_time_ms >= ? AND close_time_ms <= ?")
            sql_params.extend([now_ms, now_ms + (24 * 3600 * 1000)])

        if search:
            where_clauses.append("(title LIKE ? OR inspector_notes LIKE ?)")
            sql_params.extend([f"%{search}%", f"%{search}%"])

        if location != "all":
            where_clauses.append("location_name = ?")
            sql_params.append(location)

        if condition != "all":
            where_clauses.append("condition_type = ?")
            sql_params.append(condition)

        if min_retail:
            try:
                where_clauses.append("estimated_retail_price >= ?")
                sql_params.append(float(min_retail))
            except ValueError:
                pass

        if max_bid:
            try:
                where_clauses.append("winning_bid_amount <= ?")
                sql_params.append(float(max_bid))
            except ValueError:
                pass

        where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        sort_map = {
            "closing_soon": "close_time_ms ASC",
            "discount_desc": "discount_pct DESC",
            "retail_desc": "estimated_retail_price DESC",
            "bid_asc": "winning_bid_amount ASC"
        }
        order_sql = f" ORDER BY {sort_map.get(sort, 'close_time_ms ASC')}"

        cursor.execute(f"SELECT COUNT(*) FROM auction_items {where_sql}", sql_params)
        total_count = cursor.fetchone()[0]

        query = f"""
            SELECT id, title, condition_type, winning_bid_amount, estimated_retail_price,
                   discount_pct, close_time_ms, closing_date, location_name, inspector_notes, item_url, photo_url
            FROM auction_items
            {where_sql}
            {order_sql}
            LIMIT ? OFFSET ?
        """
        cursor.execute(query, sql_params + [limit, offset])
        items = [dict(r) for r in cursor.fetchall()]
        conn.close()

        self.send_json({
            "total": total_count,
            "page": page,
            "limit": limit,
            "items": items
        })

    def handle_api_analytics(self, model):
        from src.query_lake import PRESET_QUERIES, get_duckdb_lake_conn
        if model not in PRESET_QUERIES:
            model = "category-spread"

        preset = PRESET_QUERIES[model]
        con = get_cached_r2_conn() or get_duckdb_lake_conn(use_local=True)
        try:
            df = con.execute(preset["sql"]).df()
            cols = list(df.columns)
            rows = df.values.tolist()
            self.send_json({
                "title": preset["title"],
                "subtitle": f"Generated directly by DuckDB over Cloudflare R2 ({len(rows)} records)",
                "columns": cols,
                "rows": rows
            })
        except Exception as e:
            self.send_json({"error": str(e)}, status=500)

    def handle_api_sql(self, sql):
        from src.query_lake import get_duckdb_lake_conn
        con = get_cached_r2_conn() or get_duckdb_lake_conn(use_local=True)
        try:
            df = con.execute(sql).df()
            cols = list(df.columns)
            rows = df.values.tolist()
            self.send_json({
                "columns": cols,
                "rows": rows
            })
        except Exception as e:
            self.send_json({"error": str(e)}, status=400)


def start_server():
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", PORT), DashboardRequestHandler) as httpd:
        print(f"\n[✓] Nellis Auction Analytics Dashboard running at: http://localhost:{PORT}")
        print("Press Ctrl+C to stop the dashboard server.\n")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down dashboard server...")
            httpd.server_close()


def main():
    url = f"http://localhost:{PORT}"
    print(f"[*] Starting local server on port {PORT}...")

    def open_browser():
        time.sleep(1)
        webbrowser.open(url)

    Thread(target=open_browser, daemon=True).start()
    start_server()


if __name__ == "__main__":
    main()
