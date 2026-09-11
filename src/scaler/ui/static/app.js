/* Scaler Web GUI - Client-side application */
"use strict";

// -- State --
var ws = null;
var reconnectDelay = 500;
var workerSortField = null;  // current sort column field name (the server does the sorting)
var workerSortAsc = true;    // sort direction
var lastWorkersData = [];    // this browser's page of worker rows, already sorted by the server
var workersTotal = 0;        // full fleet size, of which this browser holds one page
var taskLogTotal = 0;  // completed tasks seen by the server since it started, uncapped by the display ring
var TASK_LOG_MAX_SIZE = 100;  // overridden by server's task_log_max_size on initial state
var taskLogData = [];        // full task-log data, newest first, up to TASK_LOG_MAX_SIZE
var taskLogById = {};        // task_id -> entry, for in-place status updates
// The worker views are paged by the server, so the browser never receives the whole fleet. The task
// log is small and bounded server-side, so it stays paged here with PAGE_SIZE.
var PAGE_SIZE = 50;
var workersPage = 0;
var workersPages = 1;
var taskLogPage = 0;
var processorsPage = 0;
var processorsPages = 1;
var processorsTotal = 0;
var streamPage = 0;
var streamPages = 1;
var streamTotal = 0;
var streamBars = [];       // bars for this page, row index already re-based page-local by the server
var streamRows = [];       // row labels (truncated) for the current page
var streamFullRows = [];   // row labels (full worker names) for the current page
var streamRowManagers = []; // manager color per row, current page
var streamManagerColors = {}; // manager_id -> color
var memoryPoints = [];     // memory chart points
var memoryScale = "linear";
var memoryYTicks = [];
var streamTicks = [];
var streamWindow = 300;    // seconds
var streamNeedsRedraw = false;
var memoryNeedsRedraw = false;
var activeTab = "live";          // currently visible tab; hidden tabs are cached, not re-rendered
var lastSchedulerData = null;    // latest cached payloads, replayed on tab switch
var lastManagersData = [];
var lastProcessorsData = [];
var streamLegendData = [];       // cached stream legend + manager legend for re-render on switch
var streamManagerLegendData = [];

// -- DOM refs --
var $ = function(id) { return document.getElementById(id); };
var connStatus = $("conn-status");
var schedAddress = $("sched-address");
var schedCpu = $("sched-cpu");
var schedRss = $("sched-rss");
var schedRssFree = $("sched-rss-free");
var schedLastSeen = $("sched-last-seen");
var managersBody = $("managers-body");
var workersBody = $("workers-body");
var workersCount = $("workers-count");
var tasklogBody = $("tasklog-body");
var tasklogCount = $("tasklog-count");
var streamCanvas = $("stream-canvas");
var streamCtx = streamCanvas.getContext("2d");
var streamContainer = $("stream-container");
var streamAxis = $("stream-axis");
var streamLegend = $("stream-legend");
var memoryCanvas = $("memory-canvas");
var memoryCtx = memoryCanvas.getContext("2d");
var processorsContainer = $("processors-container");
var tooltip = $("tooltip");

// -- Tabs --
var tabs = document.querySelectorAll(".tab");
var panels = document.querySelectorAll(".tab-panel");

for (var i = 0; i < tabs.length; i++) {
    tabs[i].addEventListener("click", (function(tab) {
        return function() {
            for (var j = 0; j < tabs.length; j++) {
                tabs[j].classList.remove("active");
                panels[j].classList.remove("active");
            }
            tab.classList.add("active");
            activeTab = tab.getAttribute("data-tab");
            var panel = $("panel-" + activeTab);
            if (panel) panel.classList.add("active");
            updateFitPageStream();
            renderActiveTab();
        };
    })(tabs[i]));
}

// Render the now-visible tab from the latest cached data. Hidden tabs are skipped on update; switching to
// a tab replays its cached payload so it is immediately current.
function renderActiveTab() {
    if (activeTab === "live") {
        if (lastSchedulerData) renderScheduler(lastSchedulerData);
        renderWorkers();
        renderManagers();
    } else if (activeTab === "tasklog") {
        renderTaskLog();
    } else if (activeTab === "processors") {
        renderProcessors();
    } else if (activeTab === "stream") {
        renderStreamStatic();
        streamNeedsRedraw = true;
        memoryNeedsRedraw = true;
    }
}

// Every paged view carries the same controls above and below its content, so paging a long table does
// not mean scrolling to the bottom for every click.
function renderPagers(elId, page, totalPages, total, onPage) {
    renderPager(elId + "-top", page, totalPages, total, onPage);
    renderPager(elId, page, totalPages, total, onPage);
}

// Numbered-page controls: renders "Prev  Page X / Y (N)  Next" into elId; onPage(newPage) re-renders the view.
// Renders nothing (hidden via CSS) when there is only one page.
function renderPager(elId, page, totalPages, total, onPage) {
    var el = $(elId);
    if (!el) return;
    if (totalPages <= 1) { el.innerHTML = ""; return; }
    el.innerHTML = "";
    var prev = document.createElement("button");
    prev.className = "pager-btn";
    prev.textContent = "‹ Prev";
    prev.disabled = page <= 0;
    prev.addEventListener("click", function() { if (page > 0) onPage(page - 1); });
    var info = document.createElement("span");
    info.className = "pager-info";
    info.textContent = "Page " + (page + 1) + " / " + totalPages + "  (" + total + ")";
    var next = document.createElement("button");
    next.className = "pager-btn";
    next.textContent = "Next ›";
    next.disabled = page >= totalPages - 1;
    next.addEventListener("click", function() { if (page < totalPages - 1) onPage(page + 1); });
    el.appendChild(prev);
    el.appendChild(info);
    el.appendChild(next);
}

// Clamp a page index and return the slice bounds ([start, end)) for the current page over `total` items.
function pageSlice(page, total, size) {
    size = size || PAGE_SIZE;
    var totalPages = Math.max(1, Math.ceil(total / size));
    if (page >= totalPages) page = totalPages - 1;
    if (page < 0) page = 0;
    return { page: page, totalPages: totalPages, start: page * size, end: page * size + size };
}

// -- Fit Page Toggle --
var fitPageBtn = $("fit-page-btn");
var fitPageActive = false;

function updateFitPageStream() {
    var streamActive = document.querySelector('.tab.active');
    var isStream = streamActive && streamActive.getAttribute('data-tab') === 'stream';
    document.body.classList.toggle('fit-page-stream', fitPageActive && isStream);
}

fitPageBtn.addEventListener("click", function() {
    fitPageActive = !fitPageActive;
    document.body.classList.toggle("fit-page", fitPageActive);
    fitPageBtn.classList.toggle("active", fitPageActive);
    updateFitPageStream();
    streamNeedsRedraw = true;
    memoryNeedsRedraw = true;
});

// -- Settings --
function setupToggle(groupId, callback) {
    var group = $(groupId);
    if (!group) return;
    var btns = group.querySelectorAll(".toggle-btn");
    for (var i = 0; i < btns.length; i++) {
        btns[i].addEventListener("click", (function(btn) {
            return function() {
                for (var j = 0; j < btns.length; j++) {
                    btns[j].classList.remove("active");
                }
                btn.classList.add("active");
                callback(btn.getAttribute("data-value"));
            };
        })(btns[i]));
    }
}

setupToggle("window-toggle", function(val) {
    sendSettings({ stream_window: parseInt(val, 10) });
});

setupToggle("scale-toggle", function(val) {
    sendSettings({ memory_scale: val });
});

function sendSettings(settings) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "settings", settings: settings }));
    }
}

// Tell the server what this browser is looking at; it answers immediately with just that view.
function sendView(view) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "view", view: view }));
    }
}

// -- WebSocket --
function connect() {
    var proto = location.protocol === "https:" ? "wss:" : "ws:";
    ws = new WebSocket(proto + "//" + location.host + "/ws");

    ws.onopen = function() {
        connStatus.textContent = "Connected";
        connStatus.classList.add("connected");
        reconnectDelay = 500;
    };

    ws.onclose = function() {
        connStatus.textContent = "Disconnected";
        connStatus.classList.remove("connected");
        setTimeout(connect, Math.min(reconnectDelay, 10000));
        reconnectDelay *= 2;
    };

    ws.onerror = function() {
        ws.close();
    };

    ws.onmessage = function(evt) {
        var data;
        try {
            data = JSON.parse(evt.data);
        } catch (e) {
            return;
        }
        handleMessage(data);
    };
}

function handleMessage(data) {
    if (data.type === "full_state") {
        handleFullState(data);
        return;
    }

    if (data.scheduler) {
        updateScheduler(data.scheduler);
    }
    if (data.workers) {
        applyPageInfo(data);
        updateWorkers(data.workers);
    }
    if (data.worker_managers) {
        updateWorkerManagers(data.worker_managers);
    }
    if (data.worker_events) {
        handleWorkerEvents(data.worker_events);
    }
    if (data.task_updates) {
        if (typeof data.task_log_total === "number") taskLogTotal = data.task_log_total;
        handleTaskUpdates(data.task_updates);
    }
    if (data.task_stream) {
        updateTaskStream(data.task_stream);
    }
    if (data.memory_chart) {
        updateMemoryChart(data.memory_chart);
    }
    if (data.processors) {
        applyPageInfo(data);
        updateProcessors(data.processors);
    }
    if (data.settings) {
        applySettings(data.settings);
    }
}

// The server clamps the page it actually served, so mirror that back rather than what we asked for.
function applyPageInfo(data) {
    if (typeof data.workers_total === "number") workersTotal = data.workers_total;
    if (typeof data.workers_page === "number") workersPage = data.workers_page;
    if (typeof data.workers_pages === "number") workersPages = data.workers_pages;
    if (typeof data.processors_total === "number") processorsTotal = data.processors_total;
    if (typeof data.processors_page === "number") processorsPage = data.processors_page;
    if (typeof data.processors_pages === "number") processorsPages = data.processors_pages;
}

function handleFullState(data) {
    if (data.scheduler) updateScheduler(data.scheduler);
    applyPageInfo(data);
    if (data.workers) updateWorkers(data.workers);
    if (data.worker_managers) updateWorkerManagers(data.worker_managers);
    if (typeof data.task_log_max_size === "number" && data.task_log_max_size > 0) {
        TASK_LOG_MAX_SIZE = data.task_log_max_size;
    }
    taskLogTotal = typeof data.task_log_total === "number" ? data.task_log_total : 0;
    if (data.task_log) {
        setTaskLog(data.task_log);
    }
    if (data.task_stream) updateTaskStream(data.task_stream);
    if (data.memory_chart) updateMemoryChart(data.memory_chart);
    if (data.processors) updateProcessors(data.processors);
    if (data.settings) applySettings(data.settings);
}

function applySettings(settings) {
    if (settings.stream_window) {
        var btns = $("window-toggle").querySelectorAll(".toggle-btn");
        for (var i = 0; i < btns.length; i++) {
            btns[i].classList.toggle("active", btns[i].getAttribute("data-value") === String(settings.stream_window));
        }
    }
    if (settings.memory_scale) {
        var btns2 = $("scale-toggle").querySelectorAll(".toggle-btn");
        for (var i = 0; i < btns2.length; i++) {
            btns2[i].classList.toggle("active", btns2[i].getAttribute("data-value") === settings.memory_scale);
        }
    }
}

// -- Live Tab: Scheduler --
function updateScheduler(sched) {
    lastSchedulerData = sched;
    if (activeTab === "live") renderScheduler(sched);
}

function renderScheduler(sched) {
    schedAddress.textContent = sched.monitor_address || "—";
    schedCpu.textContent = sched.cpu || "—";
    schedRss.textContent = sched.rss || "—";
    schedRssFree.textContent = sched.rss_free || "—";
    schedLastSeen.textContent = sched.last_seen || "—";
    // stale = the scheduler's periodic heartbeat has gone quiet; flag it so a stuck scheduler is obvious.
    schedLastSeen.classList.toggle("stale", !!sched.stale);
}

// -- Live Tab: Worker Managers --
function updateWorkerManagers(managers) {
    lastManagersData = managers;
    if (activeTab === "live") renderManagers();
}

function renderManagers() {
    var managers = lastManagersData;
    managersBody.innerHTML = "";
    if (!managers || managers.length === 0) {
        var tr = document.createElement("tr");
        var td = document.createElement("td");
        td.colSpan = 12;
        td.style.color = "#64748b";
        td.textContent = "No worker managers connected";
        tr.appendChild(td);
        managersBody.appendChild(tr);
        return;
    }
    for (var i = 0; i < managers.length; i++) {
        var m = managers[i];
        var tr = document.createElement("tr");

        var tdId = document.createElement("td");
        tdId.textContent = m.manager_id || "—";
        tr.appendChild(tdId);

        var tdAddr = document.createElement("td");
        tdAddr.textContent = m.identity || "—";
        tdAddr.title = m.identity || "";
        tr.appendChild(tdAddr);

        var tdSeen = document.createElement("td");
        tdSeen.textContent = m.last_seen || "—";
        tr.appendChild(tdSeen);

        var tdConc = document.createElement("td");
        tdConc.textContent = m.max_task_concurrency != null ? m.max_task_concurrency : "—";
        tr.appendChild(tdConc);

        var tdWC = document.createElement("td");
        tdWC.textContent = m.worker_count != null ? m.worker_count : "0";
        tr.appendChild(tdWC);

        var tdCpu = document.createElement("td");
        tdCpu.textContent = m.total_proc_cpu != null ? m.total_proc_cpu + "%" : "—";
        tr.appendChild(tdCpu);

        var tdRss = document.createElement("td");
        tdRss.textContent = m.total_proc_rss != null ? m.total_proc_rss : "—";
        tr.appendChild(tdRss);

        var tdFree = document.createElement("td");
        tdFree.textContent = m.total_free != null ? m.total_free : "—";
        tr.appendChild(tdFree);

        var tdSent = document.createElement("td");
        tdSent.textContent = m.total_sent != null ? m.total_sent : "—";
        tr.appendChild(tdSent);

        var tdQueued = document.createElement("td");
        tdQueued.textContent = m.total_queued != null ? m.total_queued : "—";
        tr.appendChild(tdQueued);

        var tdSusp = document.createElement("td");
        tdSusp.textContent = m.total_suspended != null ? m.total_suspended : "—";
        tr.appendChild(tdSusp);

        var tdCaps = document.createElement("td");
        tdCaps.textContent = m.capabilities || "—";
        tr.appendChild(tdCaps);

        managersBody.appendChild(tr);
    }
}

// -- Live Tab: Workers --
// Column order of the workers table; a header click sends the field name to the server.
var WORKER_FIELDS = ["name", "manager_id", "agt_cpu", "agt_rss", "proc_cpu", "proc_rss", "mem_used_pct",
                     "free", "sent", "queued", "suspended", "lag", "itl", "last_seen", "capabilities"];

function updateWorkers(workers) {
    lastWorkersData = workers;
    if (activeTab === "live") renderWorkers();
}

function renderWorkers() {
    // lastWorkersData is already this browser's page, sorted by the server.
    var pageRows = lastWorkersData;

    workersBody.innerHTML = "";
    for (var i = 0; i < pageRows.length; i++) {
        var row = createWorkerRow(pageRows[i]);
        updateWorkerRow(row, pageRows[i]);
        workersBody.appendChild(row);
    }
    updateWorkersCountBadge();
    renderPagers("workers-pager", workersPage, workersPages, workersTotal, function(p) {
        workersPage = p;
        sendView({ workers_page: p });
    });
}

// The badge counts the whole fleet, of which this browser holds one page.
function updateWorkersCountBadge() {
    workersCount.textContent = workersTotal;
}

// Sorting runs on the server, so a click just sets the indicator and asks for page 0 of the new order.
function setupWorkerSort() {
    var thead = workersBody.parentElement.querySelector("thead tr");
    if (!thead) return;
    var ths = thead.children;
    for (var i = 0; i < ths.length; i++) {
        ths[i].classList.add("sortable");
        ths[i].setAttribute("data-sort-field", WORKER_FIELDS[i]);
        (function(th, field) {
            th.addEventListener("click", function() {
                if (workerSortField === field) {
                    workerSortAsc = !workerSortAsc;
                } else {
                    workerSortField = field;
                    workerSortAsc = true;
                }
                // update header indicators
                var allTh = th.parentElement.children;
                for (var k = 0; k < allTh.length; k++) {
                    allTh[k].classList.remove("sort-asc", "sort-desc");
                }
                th.classList.add(workerSortAsc ? "sort-asc" : "sort-desc");
                workersPage = 0;  // jump to the top of the new sort order
                sendView({ workers_sort: field, workers_sort_ascending: workerSortAsc, workers_page: 0 });
            });
        })(ths[i], WORKER_FIELDS[i]);
    }
}
setupWorkerSort();

var WORKER_GAUGE_FIELDS = {"agt_cpu": 1, "agt_rss": 1, "proc_cpu": 1, "proc_rss": 1, "mem_used_pct": 1};

function createWorkerRow(w) {
    var tr = document.createElement("tr");
    tr.setAttribute("data-worker", w.id);
    for (var i = 0; i < WORKER_FIELDS.length; i++) {
        var td = document.createElement("td");
        td.setAttribute("data-field", WORKER_FIELDS[i]);
        if (WORKER_GAUGE_FIELDS[WORKER_FIELDS[i]]) buildGauge(td);
        tr.appendChild(td);
    }
    return tr;
}

// Static HTML gauge, still used by the processors tree (which is rebuilt wholesale anyway).
function makeGaugeHTML(value, max, unit) {
    if (max <= 0) max = 100;
    var pct = Math.min(100, (value / max) * 100);
    var cls = pct > 90 ? "critical" : pct > 70 ? "high" : "";
    return '<div class="gauge"><div class="gauge-bar"><div class="gauge-fill ' + cls +
        '" style="width:' + pct.toFixed(1) + '%"></div></div><span class="gauge-value">' +
        value + (unit || "") + '</span></div>';
}

// In-place gauge for the workers table: build the DOM once, then update width/value each tick instead of
// rebuilding the gauge HTML every refresh.
function buildGauge(td) {
    var gauge = document.createElement("div");
    gauge.className = "gauge";
    var bar = document.createElement("div");
    bar.className = "gauge-bar";
    var fill = document.createElement("div");
    fill.className = "gauge-fill";
    var value = document.createElement("span");
    value.className = "gauge-value";
    bar.appendChild(fill);
    gauge.appendChild(bar);
    gauge.appendChild(value);
    td.appendChild(gauge);
    td._gaugeFill = fill;
    td._gaugeValue = value;
}

function setGauge(td, value, max, unit) {
    if (max <= 0) max = 100;
    var pct = Math.min(100, (value / max) * 100);
    td._gaugeFill.style.width = pct.toFixed(1) + "%";
    td._gaugeFill.className = "gauge-fill" + (pct > 90 ? " critical" : pct > 70 ? " high" : "");
    td._gaugeValue.textContent = value + (unit || "");
}

function updateWorkerRow(tr, w) {
    var cells = tr.children;
    cells[0].textContent = w.name;
    cells[0].title = w.full_name || w.name;
    cells[1].textContent = w.manager_id || "—";
    setGauge(cells[2], w.agt_cpu, 100, "%");
    setGauge(cells[3], w.agt_rss, w.total_rss, "");
    setGauge(cells[4], w.proc_cpu, 100, "%");
    setGauge(cells[5], w.proc_rss, w.total_rss, "");
    setGauge(cells[6], w.mem_used_pct, 100, "%");
    cells[6].title = w.mem_limit ? (w.mem_used + " / " + w.mem_limit + " MB used") : "";
    cells[7].textContent = w.free;
    cells[8].textContent = w.sent;
    cells[9].textContent = w.queued;
    cells[10].textContent = w.suspended;
    cells[11].textContent = w.lag;
    cells[12].textContent = w.itl;
    cells[13].textContent = w.last_seen;
    cells[14].textContent = w.capabilities;
}

function handleWorkerEvents(events) {
    var removed = false;
    for (var i = 0; i < events.length; i++) {
        var ev = events[i];
        if (ev.state === "disconnected") {
            var before = lastWorkersData.length;
            lastWorkersData = lastWorkersData.filter(function(w) { return w.id !== ev.worker_id; });
            if (lastWorkersData.length !== before) removed = true;
        }
    }
    if (removed && activeTab === "live") renderWorkers();
}

// -- Task Log --
function formatTime(epoch) {
    if (!epoch) return "";
    var d = new Date(epoch * 1000);
    var h = String(d.getHours()).padStart(2, "0");
    var m = String(d.getMinutes()).padStart(2, "0");
    var s = String(d.getSeconds()).padStart(2, "0");
    return h + ":" + m + ":" + s;
}

function statusClass(status) {
    if (status === "success") return "status-success";
    if (status in {"running":1, "inactive":1, "canceling":1, "balanceCanceling":1}) return "status-running";
    return "status-fail";
}

function handleTaskUpdates(entries) {
    for (var i = 0; i < entries.length; i++) {
        var e = entries[i];
        var existing = taskLogById[e.task_id];
        if (existing) {
            for (var k in e) { if (Object.prototype.hasOwnProperty.call(e, k)) existing[k] = e[k]; }
        } else {
            taskLogData.unshift(e);  // newest first
            taskLogById[e.task_id] = e;
            while (taskLogData.length > TASK_LOG_MAX_SIZE) {
                var dropped = taskLogData.pop();
                delete taskLogById[dropped.task_id];
            }
        }
    }
    if (activeTab === "tasklog") renderTaskLog();
    else updateTaskLogBadge();  // the badge (server total) stays current even while the tab is hidden
}

// full_state: replace the whole task log (active + completed, newest first, already capped by the server).
function setTaskLog(entries) {
    taskLogData = entries.slice(0, TASK_LOG_MAX_SIZE);
    taskLogById = {};
    for (var i = 0; i < taskLogData.length; i++) taskLogById[taskLogData[i].task_id] = taskLogData[i];
    taskLogPage = 0;
    if (activeTab === "tasklog") renderTaskLog();
    else updateTaskLogBadge();
}

function renderTaskLog() {
    var pg = pageSlice(taskLogPage, taskLogData.length);
    taskLogPage = pg.page;
    var pageEntries = taskLogData.slice(pg.start, pg.end);
    tasklogBody.innerHTML = "";
    for (var i = 0; i < pageEntries.length; i++) tasklogBody.appendChild(makeTaskLogRow(pageEntries[i]));
    updateTaskLogBadge();
    renderPagers("tasklog-pager", taskLogPage, pg.totalPages, taskLogData.length, function(p) {
        taskLogPage = p;
        renderTaskLog();
    });
}

function makeCell(text) {
    var td = document.createElement("td");
    td.textContent = text == null ? "" : text;
    return td;
}

function makeTaskLogRow(e) {
    var tr = document.createElement("tr");
    tr.dataset.taskId = e.task_id;

    var tdId = document.createElement("td");
    var span = document.createElement("span");
    span.className = "task-id";
    span.textContent = e.task_id;
    span.title = e.task_id;
    span.addEventListener("click", (function(id) {
        return function() { if (navigator.clipboard) navigator.clipboard.writeText(id); };
    })(e.task_id));
    tdId.appendChild(span);
    tr.appendChild(tdId);

    tr.appendChild(makeCell(e.function));
    var tdWorker = makeCell(e.worker || "");
    tdWorker.title = e.full_worker || e.worker || "";
    tr.appendChild(tdWorker);
    tr.appendChild(makeCell(formatTime(e.time)));
    tr.appendChild(makeCell(e.duration));
    tr.appendChild(makeCell(e.peak_mem));
    var tdStatus = makeCell(e.status);
    tdStatus.className = statusClass(e.status);
    tr.appendChild(tdStatus);
    tr.appendChild(makeCell(e.capabilities));
    return tr;
}

// Badge shows the running total of completed tasks; once it passes the display cap it appends the cap it is
// windowed to, e.g. "501 (showing 500)".
function updateTaskLogBadge() {
    if (taskLogTotal > TASK_LOG_MAX_SIZE) {
        tasklogCount.textContent = taskLogTotal + " (showing " + TASK_LOG_MAX_SIZE + ")";
    } else {
        tasklogCount.textContent = taskLogTotal;
    }
}

// -- Task Stream (Canvas) --
var STREAM_LABEL_WIDTH = 120;
var STREAM_ROW_HEIGHT = 24;
var STREAM_PADDING_TOP = 4;

function updateTaskStream(data) {
    // Already one page: the server slices the rows and re-bases each bar's row index to the page.
    streamBars = data.bars || [];
    streamRows = data.rows || [];
    streamFullRows = data.full_rows || streamRows;
    streamRowManagers = data.row_managers || [];
    if (typeof data.page === "number") streamPage = data.page;
    if (typeof data.pages === "number") streamPages = data.pages;
    if (typeof data.total_rows === "number") streamTotal = data.total_rows;
    streamManagerColors = {};
    streamManagerLegendData = data.manager_legend || [];
    for (var ml = 0; ml < streamManagerLegendData.length; ml++) {
        streamManagerColors[streamManagerLegendData[ml].name] = streamManagerLegendData[ml].color;
    }
    streamTicks = data.ticks || [];
    streamWindow = data.window || 300;
    streamLegendData = data.legend || [];
    renderPagers("stream-pager", streamPage, streamPages, streamTotal, function(p) {
        streamPage = p;
        sendView({ stream_page: p });
    });

    if (activeTab === "stream") {
        renderStreamStatic();
        streamNeedsRedraw = true;
    }
}

// Rebuild the stream legend + time axis (DOM) from cached data; runs only while the stream tab is visible.
function renderStreamStatic() {
    var legend = streamLegendData;
    var managerLegend = streamManagerLegendData;
    streamLegend.innerHTML = "";

    // Manager legend (narrow swatches matching the 4px row stripe)
    if (managerLegend.length > 0) {
        for (var k = 0; k < managerLegend.length; k++) {
            var mItem = document.createElement("span");
            mItem.className = "legend-item";
            mItem.innerHTML = '<span class="legend-swatch legend-swatch-narrow" style="background:' +
                managerLegend[k].color + '"></span> ' + escapeHTML(managerLegend[k].name);
            streamLegend.appendChild(mItem);
        }
    }

    // Separator + status patterns
    if (managerLegend.length > 0) {
        var sep1 = document.createElement("span");
        sep1.className = "legend-item";
        sep1.style.color = "#94a3b8";
        sep1.textContent = "|";
        streamLegend.appendChild(sep1);
    }
    var failed = document.createElement("span");
    failed.className = "legend-item";
    failed.innerHTML = '<span class="legend-swatch pattern-x"></span> Failed';
    streamLegend.appendChild(failed);

    var canceled = document.createElement("span");
    canceled.className = "legend-item";
    canceled.innerHTML = '<span class="legend-swatch pattern-slash"></span> Canceled';
    streamLegend.appendChild(canceled);

    var schedulerFault = document.createElement("span");
    schedulerFault.className = "legend-item";
    schedulerFault.innerHTML = '<span class="legend-swatch pattern-grid"></span> Scheduler fault';
    streamLegend.appendChild(schedulerFault);

    // Capability legend (with separator)
    if (legend.length > 0) {
        var sep2 = document.createElement("span");
        sep2.className = "legend-item";
        sep2.style.color = "#94a3b8";
        sep2.textContent = "|";
        streamLegend.appendChild(sep2);
    }
    for (var i = 0; i < legend.length; i++) {
        var item = document.createElement("span");
        item.className = "legend-item";
        item.innerHTML = '<span class="legend-swatch" style="background:' + legend[i].color + '"></span> ' +
            escapeHTML(legend[i].name);
        streamLegend.appendChild(item);
    }

    // Update axis
    streamAxis.innerHTML = "";
    streamAxis.style.paddingLeft = STREAM_LABEL_WIDTH + "px";
    for (var j = 0; j < streamTicks.length; j++) {
        var tick = document.createElement("span");
        tick.textContent = streamTicks[j].label;
        streamAxis.appendChild(tick);
    }
}

function drawTaskStream() {
    var dpr = window.devicePixelRatio || 1;
    var containerWidth = streamContainer.clientWidth;
    var chartWidth = containerWidth - STREAM_LABEL_WIDTH;
    var numRows = streamRows.length;
    var canvasHeight = STREAM_PADDING_TOP + numRows * STREAM_ROW_HEIGHT + 4;

    streamCanvas.width = containerWidth * dpr;
    streamCanvas.height = canvasHeight * dpr;
    streamCanvas.style.width = containerWidth + "px";
    streamCanvas.style.height = canvasHeight + "px";
    streamCtx.setTransform(dpr, 0, 0, dpr, 0, 0);

    // Clear
    streamCtx.fillStyle = "#ffffff";
    streamCtx.fillRect(0, 0, containerWidth, canvasHeight);

    // Draw row labels and grid lines
    streamCtx.font = "11px " + getComputedStyle(document.body).fontFamily;
    streamCtx.textBaseline = "middle";
    for (var i = 0; i < numRows; i++) {
        var y = STREAM_PADDING_TOP + i * STREAM_ROW_HEIGHT;
        // alternating row bg
        if (i % 2 === 0) {
            streamCtx.fillStyle = "#f8fafc";
            streamCtx.fillRect(0, y, containerWidth, STREAM_ROW_HEIGHT);
        }
        // grid line
        streamCtx.strokeStyle = "#e2e8f0";
        streamCtx.beginPath();
        streamCtx.moveTo(STREAM_LABEL_WIDTH, y + STREAM_ROW_HEIGHT);
        streamCtx.lineTo(containerWidth, y + STREAM_ROW_HEIGHT);
        streamCtx.stroke();
        // label
        streamCtx.fillStyle = "#334155";
        streamCtx.fillText(streamRows[i], 4, y + STREAM_ROW_HEIGHT / 2);
        // manager color stripe
        var mgr = streamRowManagers[i];
        if (mgr && streamManagerColors[mgr]) {
            streamCtx.fillStyle = streamManagerColors[mgr];
            streamCtx.fillRect(0, y, 4, STREAM_ROW_HEIGHT);
        }
    }

    // Helper: compute bar geometry from sublane fields
    function barGeom(bar) {
        var fullBarHeight = STREAM_ROW_HEIGHT - 4;
        var sn = bar.sn || 1;
        var sl = bar.sl || 0;
        var laneHeight = fullBarHeight / sn;
        var bh = bar.p === "/" ? Math.floor(laneHeight / 2) : laneHeight;
        var laneY = STREAM_PADDING_TOP + bar.r * STREAM_ROW_HEIGHT + 2 + sl * laneHeight;
        var ry = laneY + (laneHeight - bh);
        var x1 = STREAM_LABEL_WIDTH + ((bar.x + streamWindow) / streamWindow) * chartWidth;
        var x2 = STREAM_LABEL_WIDTH + ((bar.x + bar.w + streamWindow) / streamWindow) * chartWidth;
        return { x: x1, y: ry, w: Math.max(x2 - x1, 1), h: bh, lh: laneHeight, ly: laneY };
    }

    function drawBarFill(bar, g) {
        var colors = bar.cs;
        if (colors.length === 1) {
            streamCtx.fillStyle = colors[0];
            streamCtx.fillRect(g.x, g.y, g.w, g.h);
        } else {
            var stripeW = 6;
            var cx = 0;
            var ci = 0;
            while (cx < g.w) {
                var sw = Math.min(stripeW, g.w - cx);
                streamCtx.fillStyle = colors[ci % colors.length];
                streamCtx.fillRect(g.x + cx, g.y, sw, g.h);
                cx += sw;
                ci++;
            }
        }
    }

    // Draw bars in 3 passes for correct layering:
    //   Pass 1: Running bars (bottom layer)
    //   Pass 2: Completed bars - newest first, oldest on top
    //   Pass 3: Cancelled bars on top so they're always visible

    // Pass 1: Running bars (fill + outline, bottom layer)
    for (var j = 0; j < streamBars.length; j++) {
        var bar = streamBars[j];
        if (!bar.rn) continue;
        var g = barGeom(bar);
        drawBarFill(bar, g);
        if (bar.ow > 0) {
            streamCtx.strokeStyle = bar.oc;
            streamCtx.lineWidth = bar.ow;
            streamCtx.strokeRect(g.x, g.ly, g.w, g.lh);
        }
    }

    // Pass 2: Non-cancelled completed bars - newest first (behind), oldest last (on top)
    var completedBars = [];
    for (var j = 0; j < streamBars.length; j++) {
        var bar = streamBars[j];
        if (!bar.rn && bar.p !== "/") completedBars.push(bar);
    }
    completedBars.sort(function(a, b) { return b.x - a.x; });

    for (var j = 0; j < completedBars.length; j++) {
        var bar = completedBars[j];
        var g = barGeom(bar);
        drawBarFill(bar, g);
        if (bar.p === "x") {
            drawCrossHatch(streamCtx, g.x, g.y, g.w, g.h);
        } else if (bar.p === "+") {
            drawGridHatch(streamCtx, g.x, g.y, g.w, g.h);
        }
        if (bar.ow > 0) {
            streamCtx.strokeStyle = bar.oc;
            streamCtx.lineWidth = bar.ow;
            streamCtx.strokeRect(g.x, g.ly, g.w, g.lh);
        }
    }

    // Pass 3: Cancelled bars on top so they're visible over completed bars
    for (var j = 0; j < streamBars.length; j++) {
        var bar = streamBars[j];
        if (bar.rn || bar.p !== "/") continue;
        var g = barGeom(bar);
        drawBarFill(bar, g);
        drawSlashHatch(streamCtx, g.x, g.y, g.w, g.h);
        if (bar.ow > 0) {
            streamCtx.strokeStyle = bar.oc;
            streamCtx.lineWidth = bar.ow;
            streamCtx.strokeRect(g.x, g.y, g.w, g.h);
        }
    }

    streamCtx.lineWidth = 1;
}

function drawCrossHatch(ctx, x, y, w, h) {
    ctx.save();
    ctx.beginPath();
    ctx.rect(x, y, w, h);
    ctx.clip();
    ctx.strokeStyle = "rgba(0,0,0,0.5)";
    ctx.lineWidth = 1;
    var step = 6;
    for (var i = -h; i < w + h; i += step) {
        ctx.beginPath();
        ctx.moveTo(x + i, y);
        ctx.lineTo(x + i + h, y + h);
        ctx.stroke();
        ctx.beginPath();
        ctx.moveTo(x + i + h, y);
        ctx.lineTo(x + i, y + h);
        ctx.stroke();
    }
    ctx.restore();
}

function drawGridHatch(ctx, x, y, w, h) {
    ctx.save();
    ctx.beginPath();
    ctx.rect(x, y, w, h);
    ctx.clip();
    ctx.strokeStyle = "rgba(0,0,0,0.5)";
    ctx.lineWidth = 1;
    var step = 6;
    for (var i = 0; i < w; i += step) {
        ctx.beginPath();
        ctx.moveTo(x + i, y);
        ctx.lineTo(x + i, y + h);
        ctx.stroke();
    }
    for (var j = 0; j < h; j += step) {
        ctx.beginPath();
        ctx.moveTo(x, y + j);
        ctx.lineTo(x + w, y + j);
        ctx.stroke();
    }
    ctx.restore();
}

function drawSlashHatch(ctx, x, y, w, h) {
    ctx.save();
    ctx.beginPath();
    ctx.rect(x, y, w, h);
    ctx.clip();
    ctx.strokeStyle = "rgba(0,0,0,0.5)";
    ctx.lineWidth = 1;
    var step = 6;
    for (var i = -h; i < w + h; i += step) {
        ctx.beginPath();
        ctx.moveTo(x + i + h, y);
        ctx.lineTo(x + i, y + h);
        ctx.stroke();
    }
    ctx.restore();
}

// Stream hover tooltip
streamCanvas.addEventListener("mousemove", function(evt) {
    var rect = streamCanvas.getBoundingClientRect();
    var mx = evt.clientX - rect.left;
    var my = evt.clientY - rect.top;

    var containerWidth = streamContainer.clientWidth;
    var chartWidth = containerWidth - STREAM_LABEL_WIDTH;

    for (var i = streamBars.length - 1; i >= 0; i--) {
        var bar = streamBars[i];
        var fullBarHeight = STREAM_ROW_HEIGHT - 4;
        var sn = bar.sn || 1;
        var sl = bar.sl || 0;
        var laneHeight = fullBarHeight / sn;
        var barHeight = bar.p === "/" ? Math.floor(laneHeight / 2) : laneHeight;
        var laneY = STREAM_PADDING_TOP + bar.r * STREAM_ROW_HEIGHT + 2 + sl * laneHeight;
        var rowY = laneY + (laneHeight - barHeight);
        var x1 = STREAM_LABEL_WIDTH + ((bar.x + streamWindow) / streamWindow) * chartWidth;
        var x2 = STREAM_LABEL_WIDTH + ((bar.x + bar.w + streamWindow) / streamWindow) * chartWidth;

        if (mx >= x1 && mx <= x2 && my >= rowY && my <= rowY + barHeight) {
            tooltip.textContent = bar.h;
            tooltip.style.left = (evt.clientX + 10) + "px";
            tooltip.style.top = (evt.clientY - 30) + "px";
            tooltip.classList.add("visible");
            return;
        }
    }

    // If not over a bar, check if hovering over a row label
    if (mx < STREAM_LABEL_WIDTH) {
        for (var r = 0; r < streamFullRows.length; r++) {
            var ry = STREAM_PADDING_TOP + r * STREAM_ROW_HEIGHT;
            if (my >= ry && my < ry + STREAM_ROW_HEIGHT) {
                streamCanvas.title = streamFullRows[r];
                tooltip.classList.remove("visible");
                return;
            }
        }
    }

    streamCanvas.title = "";
    tooltip.classList.remove("visible");
});

streamCanvas.addEventListener("mouseleave", function() {
    tooltip.classList.remove("visible");
});

// -- Memory Chart (Canvas) --
var MEM_LABEL_WIDTH = 80;
var MEM_PADDING = { top: 20, right: 20, bottom: 30, left: MEM_LABEL_WIDTH };

function updateMemoryChart(data) {
    memoryPoints = data.points || [];
    memoryYTicks = data.y_ticks || [];
    memoryScale = data.scale || "linear";
    streamWindow = data.window || streamWindow;
    if (activeTab === "stream") memoryNeedsRedraw = true;
}

function drawMemoryChart() {
    var container = memoryCanvas.parentElement;
    var dpr = window.devicePixelRatio || 1;
    var cw = container.clientWidth;
    var ch = container.clientHeight;

    memoryCanvas.width = cw * dpr;
    memoryCanvas.height = ch * dpr;
    memoryCanvas.style.width = cw + "px";
    memoryCanvas.style.height = ch + "px";
    memoryCtx.setTransform(dpr, 0, 0, dpr, 0, 0);

    var plotLeft = MEM_PADDING.left;
    var plotTop = MEM_PADDING.top;
    var plotWidth = cw - MEM_PADDING.left - MEM_PADDING.right;
    var plotHeight = ch - MEM_PADDING.top - MEM_PADDING.bottom;

    // Clear
    memoryCtx.fillStyle = "#ffffff";
    memoryCtx.fillRect(0, 0, cw, ch);

    if (memoryPoints.length === 0) {
        memoryCtx.fillStyle = "#94a3b8";
        memoryCtx.font = "13px " + getComputedStyle(document.body).fontFamily;
        memoryCtx.textAlign = "center";
        memoryCtx.fillText("No memory data", cw / 2, ch / 2);
        return;
    }

    // Determine y range
    var maxY = 0;
    for (var i = 0; i < memoryPoints.length; i++) {
        if (memoryPoints[i].y > maxY) maxY = memoryPoints[i].y;
    }
    maxY = Math.max(maxY, 1024 * 1024 * 1024); // min 1GB

    function mapX(val) {
        return plotLeft + ((val + streamWindow) / streamWindow) * plotWidth;
    }

    function mapY(val) {
        if (memoryScale === "log") {
            if (val <= 0) return plotTop + plotHeight;
            var logMax = Math.log10(maxY);
            var logVal = Math.log10(Math.max(val, 1));
            return plotTop + plotHeight - (logVal / logMax) * plotHeight;
        }
        return plotTop + plotHeight - (val / maxY) * plotHeight;
    }

    // Grid lines
    memoryCtx.strokeStyle = "#e2e8f0";
    memoryCtx.lineWidth = 1;
    memoryCtx.font = "10px " + getComputedStyle(document.body).fontFamily;
    memoryCtx.textAlign = "right";
    memoryCtx.textBaseline = "middle";
    memoryCtx.fillStyle = "#64748b";

    for (var t = 0; t < memoryYTicks.length; t++) {
        var ty = mapY(memoryYTicks[t].val);
        memoryCtx.beginPath();
        memoryCtx.moveTo(plotLeft, ty);
        memoryCtx.lineTo(plotLeft + plotWidth, ty);
        memoryCtx.stroke();
        memoryCtx.fillText(memoryYTicks[t].label, plotLeft - 6, ty);
    }

    // X axis ticks
    memoryCtx.textAlign = "center";
    memoryCtx.textBaseline = "top";
    for (var s = 0; s < streamTicks.length; s++) {
        var tx = mapX(streamTicks[s].val);
        memoryCtx.beginPath();
        memoryCtx.moveTo(tx, plotTop);
        memoryCtx.lineTo(tx, plotTop + plotHeight);
        memoryCtx.stroke();
        memoryCtx.fillText(streamTicks[s].label, tx, plotTop + plotHeight + 4);
    }

    // Draw filled area
    memoryCtx.beginPath();
    memoryCtx.moveTo(mapX(memoryPoints[0].x), mapY(0));
    for (var p = 0; p < memoryPoints.length; p++) {
        memoryCtx.lineTo(mapX(memoryPoints[p].x), mapY(memoryPoints[p].y));
    }
    memoryCtx.lineTo(mapX(memoryPoints[memoryPoints.length - 1].x), mapY(0));
    memoryCtx.closePath();
    memoryCtx.fillStyle = "rgba(59, 130, 246, 0.3)";
    memoryCtx.fill();

    // Draw line
    memoryCtx.beginPath();
    for (var q = 0; q < memoryPoints.length; q++) {
        var px = mapX(memoryPoints[q].x);
        var py = mapY(memoryPoints[q].y);
        if (q === 0) memoryCtx.moveTo(px, py);
        else memoryCtx.lineTo(px, py);
    }
    memoryCtx.strokeStyle = "#3b82f6";
    memoryCtx.lineWidth = 2;
    memoryCtx.stroke();

    memoryCtx.lineWidth = 1;
}

// Memory hover
memoryCanvas.addEventListener("mousemove", function(evt) {
    if (memoryPoints.length === 0) return;
    var rect = memoryCanvas.getBoundingClientRect();
    var mx = evt.clientX - rect.left;
    var container = memoryCanvas.parentElement;
    var cw = container.clientWidth;
    var plotWidth = cw - MEM_PADDING.left - MEM_PADDING.right;

    // convert mx to time
    var t = ((mx - MEM_PADDING.left) / plotWidth) * streamWindow - streamWindow;

    // find closest point
    var closest = null;
    var minDist = Infinity;
    for (var i = 0; i < memoryPoints.length; i++) {
        var d = Math.abs(memoryPoints[i].x - t);
        if (d < minDist) {
            minDist = d;
            closest = memoryPoints[i];
        }
    }

    if (closest && minDist < streamWindow * 0.05) {
        tooltip.textContent = formatBytes(closest.y) + " at " + closest.x.toFixed(1) + "s";
        tooltip.style.left = (evt.clientX + 10) + "px";
        tooltip.style.top = (evt.clientY - 30) + "px";
        tooltip.classList.add("visible");
    } else {
        tooltip.classList.remove("visible");
    }
});

memoryCanvas.addEventListener("mouseleave", function() {
    tooltip.classList.remove("visible");
});

// -- Worker Processors --
var processorsCollapsed = {};  // track collapsed state by worker name
var managerCollapsed = {};    // track collapsed state by manager id

function updateProcessors(processors) {
    lastProcessorsData = processors;
    if (activeTab === "processors") renderProcessors();
}

function renderProcessors() {
    // Each group carries fleet-wide summary numbers, but only this page's worker detail.
    var groups = lastProcessorsData || [];

    processorsContainer.innerHTML = "";
    if (processorsTotal === 0) {
        processorsContainer.innerHTML = '<div class="card"><p style="color:#64748b">No workers connected</p></div>';
        renderPagers("processors-pager", 0, 1, 0, function() {});
        return;
    }

    for (var g = 0; g < groups.length; g++) {
        var group = groups[g];
        if (!group.workers || group.workers.length === 0) continue;
        var section = buildManagerSection(group);
        processorsContainer.appendChild(section);
        for (var i = 0; i < group.workers.length; i++) {
            section.appendChild(buildWorkerProcessorDetail(group.workers[i]));
        }
    }
    renderPagers("processors-pager", processorsPage, processorsPages, processorsTotal, function(p) {
        processorsPage = p;
        sendView({ processors_page: p });
    });
}

function buildManagerSection(group) {
    var managerSection = document.createElement("details");
    managerSection.className = "manager-group card";
    managerSection.open = !managerCollapsed[group.manager_id];

    var managerSummary = document.createElement("summary");
    managerSummary.className = "manager-header";
    managerSummary.innerHTML =
        '<span class="manager-title">Manager: ' + escapeHTML(group.manager_id) + '</span>' +
        '<span class="manager-stats">' +
            '<span class="manager-stat"><b>Workers:</b> ' + group.worker_count + '</span>' +
            '<span class="manager-stat"><b>Processors:</b> ' + group.active_processors + ' active</span>' +
            '<span class="manager-stat"><b>Total PSS:</b> ' + group.total_rss + ' MB</span>' +
            '<span class="manager-stat"><b>Total CPU:</b> ' + group.total_cpu + '%</span>' +
        '</span>';
    managerSection.appendChild(managerSummary);
    (function(mid, el) {
        el.addEventListener("toggle", function() { managerCollapsed[mid] = !el.open; });
    })(group.manager_id, managerSection);
    return managerSection;
}

function buildWorkerProcessorDetail(wp) {
    var details = document.createElement("details");
    details.className = "card processor-group";
    details.open = !processorsCollapsed[wp.name];

    var summary = document.createElement("summary");
    summary.textContent = "Worker " + wp.name;
    summary.title = wp.full_name || wp.name;
    details.appendChild(summary);
    (function(name, el) {
        el.addEventListener("toggle", function() { processorsCollapsed[name] = !el.open; });
    })(wp.name, details);

    var table = document.createElement("table");
    table.className = "data-table";
    var thead = document.createElement("thead");
    var headerRow = document.createElement("tr");
    // memory columns are PSS on Linux, RSS on macOS/Windows (see get_process_memory)
    var headers = ["PID", "CPU %", "PSS (MB)", "Max PSS (MB)", "Initialized", "Has Task", "Suspended"];
    for (var h = 0; h < headers.length; h++) {
        var th = document.createElement("th");
        th.textContent = headers[h];
        headerRow.appendChild(th);
    }
    thead.appendChild(headerRow);
    table.appendChild(thead);

    var tbody = document.createElement("tbody");
    for (var p = 0; p < wp.processors.length; p++) {
        var proc = wp.processors[p];
        var tr = document.createElement("tr");
        var tdPid = document.createElement("td"); tdPid.textContent = proc.pid; tr.appendChild(tdPid);
        var tdCpu = document.createElement("td"); tdCpu.innerHTML = makeGaugeHTML(proc.cpu, 100, "%"); tr.appendChild(tdCpu);
        var tdRss = document.createElement("td"); tdRss.innerHTML = makeGaugeHTML(proc.rss, proc.rss_max_gauge, ""); tr.appendChild(tdRss);
        var tdMax = document.createElement("td"); tdMax.innerHTML = makeGaugeHTML(proc.max_rss, proc.rss_max_gauge, ""); tr.appendChild(tdMax);
        var tdInit = document.createElement("td"); tdInit.innerHTML = boolIndicator(proc.initialized); tr.appendChild(tdInit);
        var tdTask = document.createElement("td"); tdTask.innerHTML = boolIndicator(proc.has_task); tr.appendChild(tdTask);
        var tdSusp = document.createElement("td"); tdSusp.innerHTML = boolIndicator(proc.suspended); tr.appendChild(tdSusp);
        tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    details.appendChild(table);
    return details;
}

function boolIndicator(val) {
    return '<span class="bool-indicator ' + (val ? "bool-true" : "bool-false") + '"></span>';
}

// -- Utilities --
function escapeHTML(str) {
    var div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
}

function formatBytes(bytes) {
    if (bytes === 0) return "0B";
    var units = ["B", "K", "M", "G", "T"];
    var mod = 1024;
    for (var i = 0; i < units.length; i++) {
        if (bytes < mod) {
            if (i < 2) return Math.round(bytes) + units[i];
            return bytes.toFixed(1) + units[i];
        }
        bytes /= mod;
    }
    return bytes.toFixed(1) + "T";
}

// -- Animation Loop --
function renderLoop() {
    // Only the visible stream tab draws; hidden canvases are never touched (they redraw on switch-in).
    if (activeTab === "stream") {
        if (streamNeedsRedraw) {
            streamNeedsRedraw = false;
            drawTaskStream();
        }
        if (memoryNeedsRedraw) {
            memoryNeedsRedraw = false;
            drawMemoryChart();
        }
    }
    requestAnimationFrame(renderLoop);
}

// -- Resize handling --
window.addEventListener("resize", function() {
    streamNeedsRedraw = true;
    memoryNeedsRedraw = true;
});

// -- Start --
connect();
requestAnimationFrame(renderLoop);
