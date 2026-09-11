/* Scaler Web GUI - Client-side application */
"use strict";

// -- State --
var browserId = null;   // this browser's id on the server, given with its first full state
var lastWorkersData = [];    // this browser's page of worker rows, already sorted by the server
var workersTotal = 0;        // full fleet size, of which this browser holds one page
var taskLogTotal = 0;  // completed tasks the server has seen since it started, however few it retains
var taskLogHeld = 0;   // tasks the server still holds, of which this browser has one page
var taskLogData = [];  // this browser's page of task rows, newest first
// Every table is paged by the server, so the browser holds one page rather than the whole history.
var workersPage = 0;
var workersPages = 1;
var taskLogPage = 0;
var taskLogPages = 1;
var workerDetailsPage = 0;
var workerDetailsPages = 1;
var workerDetailsTotal = 0;
var objectsPage = 0;
var objectsPages = 1;
var objectsHeld = 0;
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
var lastWorkerDetails = [];
var streamLegendData = [];       // cached stream legend + manager legend for re-render on switch
var streamManagerLegendData = [];
// What this tab shows, kept across a reload and sent with every new stream so it opens on the same view.
var STATE_STORAGE_KEY = "scaler-web-gui";
var RECONNECT_DELAY_MS = 2000;
var saved = loadSavedState();

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
var workerDetailsContainer = $("workerdetails-container");
var workerDetailsCount = $("workerdetails-total");
var machinesBody = $("machines-body");
var ossObjects = $("oss-objects");
var ossUnique = $("oss-unique");
var ossSize = $("oss-size");
var ossShared = $("oss-shared");
var ossPending = $("oss-pending");
var ossOldest = $("oss-oldest");
var lastStorageData = null;
var taskEventsBody = $("taskevents-body");
var taskEventsCount = $("taskevents-count");
var taskEventsClear = $("taskevents-clear");
var taskEventsFilterLabel = $("taskevents-filter-label");
var lastTaskEvents = [];     // this browser's page of event rows, newest first
var taskEventsPage = 0;
var taskEventsPages = 1;
var taskEventsHeld = 0;      // events the server holds under the current filter
var taskEventFilter = "";    // task id the server is filtering to, empty for every task
var machinesTotal = $("machines-total");
var lastMachinesData = [];
var clientsBody = $("clients-body");
var clientsTotal = $("clients-total");
var lastClientsData = [];
var objectsBody = $("objects-body");
var objectsTotal = $("objects-total");
var lastObjectsData = [];
var lastObjectsTotal = 0;
var tooltip = $("tooltip");

// -- Tabs --
var tabs = document.querySelectorAll(".tab");
var panels = document.querySelectorAll(".tab-panel");

for (var i = 0; i < tabs.length; i++) {
    tabs[i].addEventListener("click", (function(tab) {
        return function() { selectTab(tab.getAttribute("data-tab")); };
    })(tabs[i]));
}

function selectTab(name) {
    for (var j = 0; j < tabs.length; j++) {
        tabs[j].classList.toggle("active", tabs[j].getAttribute("data-tab") === name);
        panels[j].classList.remove("active");
    }
    activeTab = name;
    saved.tab = name;
    saveState();
    var panel = $("panel-" + name);
    if (panel) panel.classList.add("active");
    updateFitPageStream();
    renderActiveTab();
}

// Render the now-visible tab from the latest cached data. Hidden tabs are skipped on update; switching to
// a tab replays its cached payload so it is immediately current.
function renderActiveTab() {
    if (activeTab === "live") {
        if (lastSchedulerData) renderScheduler(lastSchedulerData);
        if (lastStorageData) renderStorage(lastStorageData);
        renderWorkers();
        renderManagers();
    } else if (activeTab === "tasklist") {
        renderTaskLog();
    } else if (activeTab === "tasklog") {
        renderTaskEvents();
    } else if (activeTab === "workers") {
        renderWorkerDetails();
    } else if (activeTab === "machines") {
        renderMachines();
    } else if (activeTab === "clients") {
        renderClients();
    } else if (activeTab === "objects") {
        renderObjects();
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
    Object.assign(saved.settings, settings);
    saveState();
    postView({ settings: settings });
}

// Tell the server what this browser is looking at. It answers with just that view.
function sendView(view) {
    Object.assign(saved.view, view);
    saveState();
    postView({ view: view });
}

function loadSavedState() {
    var state = null;
    try {
        state = JSON.parse(sessionStorage.getItem(STATE_STORAGE_KEY));
    } catch (e) {}
    if (!state || typeof state !== "object") state = {};
    return { tab: state.tab || "live", view: state.view || {}, settings: state.settings || {} };
}

// Without storage the view lasts as long as the page does.
function saveState() {
    try {
        sessionStorage.setItem(STATE_STORAGE_KEY, JSON.stringify(saved));
    } catch (e) {}
}

// The stream is one-way, so a view change is a request of its own; browserId says whose view to move.
function postView(body) {
    if (browserId === null) return;
    body.browser_id = browserId;
    fetch("/view", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
    }).then(function(response) {
        return response.ok ? response.json() : null;
    }).then(function(data) {
        if (data) handleMessage(data);
    }).catch(function() {});
}

// -- Server-sent events --
// A new stream opens on the view this tab saved, so a reload or a dropped stream comes back to what it showed.
// EventSource would retry the URL it was built with, which holds the view of that moment, so this reconnects itself.
function connect() {
    var source = new EventSource("/events?state=" + encodeURIComponent(JSON.stringify(saved)));

    source.onopen = function() {
        connStatus.textContent = "Connected";
        connStatus.classList.add("connected");
    };

    source.onerror = function() {
        connStatus.textContent = "Disconnected";
        connStatus.classList.remove("connected");
        source.close();
        setTimeout(connect, RECONNECT_DELAY_MS);
    };

    source.onmessage = function(evt) {
        var data;
        try {
            data = JSON.parse(evt.data);
        } catch (e) {
            return;
        }
        handleMessage(data);
    };
}

// A full state and an update carry the same sections, the full state adding only the browser id.
function handleMessage(data) {
    if (data.type === "full_state" && typeof data.browser_id === "number") browserId = data.browser_id;

    applyPageInfo(data);
    if (data.scheduler) updateScheduler(data.scheduler);
    if (data.workers) updateWorkers(data.workers);
    if (data.machines) updateMachines(data.machines);
    if (data.clients) updateClients(data.clients);
    if (data.storage) updateStorage(data.storage);
    if (data.objects) updateObjects(data.objects, data.objects_total);
    if (data.task_log) updateTaskLog(data.task_log);
    if (data.task_events) updateTaskEvents(data.task_events);
    if (data.worker_managers) updateWorkerManagers(data.worker_managers);
    if (data.worker_events) handleWorkerEvents(data.worker_events);
    if (data.task_stream) updateTaskStream(data.task_stream);
    if (data.memory_chart && data.memory_chart.cpu_points) lastCpuPoints = data.memory_chart.cpu_points;
    if (data.memory_chart) updateMemoryChart(data.memory_chart);
    if (data.worker_details) updateWorkerDetails(data.worker_details);
    if (data.settings) applySettings(data.settings);
}

// The server clamps the page it actually served, so mirror that back rather than what we asked for.
function applyPageInfo(data) {
    if (typeof data.workers_total === "number") workersTotal = data.workers_total;
    if (typeof data.workers_page === "number") workersPage = data.workers_page;
    if (typeof data.workers_pages === "number") workersPages = data.workers_pages;
    if (typeof data.worker_details_total === "number") workerDetailsTotal = data.worker_details_total;
    if (typeof data.worker_details_page === "number") workerDetailsPage = data.worker_details_page;
    if (typeof data.worker_details_pages === "number") workerDetailsPages = data.worker_details_pages;
    if (typeof data.objects_held === "number") objectsHeld = data.objects_held;
    if (typeof data.objects_page === "number") objectsPage = data.objects_page;
    if (typeof data.objects_pages === "number") objectsPages = data.objects_pages;
    if (typeof data.task_log_total === "number") taskLogTotal = data.task_log_total;
    if (typeof data.task_log_held === "number") taskLogHeld = data.task_log_held;
    if (typeof data.task_log_page === "number") taskLogPage = data.task_log_page;
    if (typeof data.task_log_pages === "number") taskLogPages = data.task_log_pages;
    if (typeof data.task_events_held === "number") taskEventsHeld = data.task_events_held;
    if (typeof data.task_events_page === "number") taskEventsPage = data.task_events_page;
    if (typeof data.task_events_pages === "number") taskEventsPages = data.task_events_pages;
    if (typeof data.task_events_task === "string") taskEventFilter = data.task_events_task;
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

// -- Live Tab: Object Storage --
function updateStorage(storage) {
    lastStorageData = storage;
    if (activeTab === "live") renderStorage(storage);
}

// A pending count that does not fall is a fetch nobody can answer: get_object waits without a bound.
function renderStorage(storage) {
    ossObjects.textContent = storage.objects;
    ossUnique.textContent = storage.unique_objects;
    ossSize.textContent = storage.size;
    ossShared.textContent = storage.shared;
    ossPending.textContent = storage.pending + (storage.pending ? " (" + storage.pending_objects + " objects)" : "");
    ossPending.classList.toggle("stale", storage.pending > 0);
    ossOldest.textContent = storage.oldest_pending;
    ossOldest.classList.toggle("stale", storage.pending > 0);
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
var WORKER_FIELDS = ["name", "manager_id", "host", "task", "task_age", "agt_cpu", "agt_rss", "proc_cpu",
                     "proc_rss", "mem_used_pct", "free", "sent", "queued", "suspended", "lag", "itl",
                     "last_seen", "capabilities"];

function updateWorkers(workers) {
    lastWorkersData = workers;
    if (activeTab === "live") renderWorkers();
}

var MACHINE_FIELDS = ["host", "workers", "busy", "idle", "managers", "cpu", "rss", "rss_free",
                      "mem_used_pct", "queued", "sent", "net_sent", "net_recv"];

var TASK_EVENT_FIELDS = ["time", "task_id", "event", "client", "worker", "function", "detail"];

// Column order of the task list, shared by its header row and the cells below it.
var TASK_LOG_FIELDS = ["task_id", "function", "client", "worker", "time", "duration", "peak_mem", "objects",
                       "status", "capabilities"];

var lastCpuPoints = [];
var lastCpuTicks = [];  // the right axis, from 0 up to its last tick

function updateTaskEvents(rows) {
    lastTaskEvents = rows;
    if (activeTab === "tasklog") renderTaskEvents();
}

// Filtering to one task and paging both run on the server, so this renders the page it was handed.
function renderTaskEvents() {
    taskEventsBody.innerHTML = "";
    for (var i = 0; i < lastTaskEvents.length; i++) {
        var ev = lastTaskEvents[i];
        var tr = document.createElement("tr");
        tr.className = "clickable";
        tr.title = "Click to show only this task";
        (function(taskId) {
            tr.addEventListener("click", function() { showOnlyTask(taskId); });
        })(ev.task_id);
        for (var f = 0; f < TASK_EVENT_FIELDS.length; f++) {
            var td = document.createElement("td");
            var value = ev[TASK_EVENT_FIELDS[f]];
            if (TASK_EVENT_FIELDS[f] === "task_id" && value) value = value.slice(0, 12);
            td.textContent = (value === undefined || value === null || value === "") ? "\u2014" : value;
            tr.appendChild(td);
        }
        taskEventsBody.appendChild(tr);
    }
    if (taskEventsCount) taskEventsCount.textContent = "(" + taskEventsHeld + ")";
    if (taskEventsClear) taskEventsClear.style.display = taskEventFilter ? "" : "none";
    if (taskEventsFilterLabel) {
        taskEventsFilterLabel.textContent = taskEventFilter ? "filtered to " + taskEventFilter.slice(0, 12) : "";
    }
    renderPagers("taskevents-pager", taskEventsPage, taskEventsPages, taskEventsHeld, function(p) {
        taskEventsPage = p;
        sendView({ task_events_page: p });
    });
}

function showOnlyTask(taskId) {
    taskEventFilter = taskId;
    taskEventsPage = 0;
    sendView({ task_events_task: taskId, task_events_page: 0 });
}

if (taskEventsClear) {
    taskEventsClear.addEventListener("click", function() { showOnlyTask(""); });
}

function updateMachines(machines) {
    lastMachinesData = machines || [];
    if (activeTab === "machines") renderMachines();
}

function renderMachines() {
    machinesBody.innerHTML = "";
    for (var i = 0; i < lastMachinesData.length; i++) {
        var m = lastMachinesData[i];
        var tr = document.createElement("tr");
        for (var f = 0; f < MACHINE_FIELDS.length; f++) {
            var td = document.createElement("td");
            var value = m[MACHINE_FIELDS[f]];
            td.textContent = (value === undefined || value === null) ? "\u2014" : value;
            tr.appendChild(td);
        }
        machinesBody.appendChild(tr);
    }
    if (machinesTotal) machinesTotal.textContent = lastMachinesData.length ? "(" + lastMachinesData.length + ")" : "";
}

var CLIENT_FIELDS = ["client", "host", "tasks", "finished", "failed", "cpu", "rss", "latency",
                     "connected", "last_seen"];

function updateClients(clients) {
    lastClientsData = clients || [];
    if (activeTab === "clients") renderClients();
}

function renderClients() {
    clientsBody.innerHTML = "";
    for (var i = 0; i < lastClientsData.length; i++) {
        var c = lastClientsData[i];
        var tr = document.createElement("tr");
        for (var f = 0; f < CLIENT_FIELDS.length; f++) {
            var td = document.createElement("td");
            var value = c[CLIENT_FIELDS[f]];
            td.textContent = (value === undefined || value === null) ? "\u2014" : value;
            if (CLIENT_FIELDS[f] === "client") td.title = c.full_client || "";
            tr.appendChild(td);
        }
        clientsBody.appendChild(tr);
    }
    if (clientsTotal) clientsTotal.textContent = lastClientsData.length ? "(" + lastClientsData.length + ")" : "";
}

function updateObjects(objects, total) {
    lastObjectsData = objects || [];
    if (typeof total === "number") lastObjectsTotal = total;
    if (activeTab === "objects") renderObjects();
}

// The scheduler sends the biggest objects it tracks, so the pager walks those rather than the whole store.
function renderObjectsCount() {
    if (!objectsTotal) return;
    objectsTotal.textContent = objectsHeld < lastObjectsTotal
        ? "(" + objectsHeld + " biggest of " + lastObjectsTotal + ")"
        : "(" + lastObjectsTotal + ")";
}

var OBJECT_FIELDS = ["object", "name", "type", "size", "client", "tasks"];

// What each column's tooltip carries, when the cell itself is a shortened form.
var OBJECT_TITLE_FIELDS = {
    "object": function(o) { return o.object_id || ""; },
    "name": function(o) { return o.full_name || ""; },
    "client": function(o) { return o.full_client || ""; },
    "tasks": function(o) { return (o.task_ids || []).join(" "); }
};

function renderObjects() {
    objectsBody.innerHTML = "";
    for (var i = 0; i < lastObjectsData.length; i++) {
        var o = lastObjectsData[i];
        var tr = document.createElement("tr");
        for (var f = 0; f < OBJECT_FIELDS.length; f++) {
            var field = OBJECT_FIELDS[f];
            var td = document.createElement("td");
            var value = o[field];
            var title = OBJECT_TITLE_FIELDS[field];
            if (title) td.title = title(o);
            td.textContent = (value === undefined || value === null || value === "") ? "\u2014" : value;
            tr.appendChild(td);
        }
        objectsBody.appendChild(tr);
    }
    renderObjectsCount();
    renderPagers("objects-pager", objectsPage, objectsPages, objectsHeld, function(p) {
        objectsPage = p;
        sendView({ objects_page: p });
    });
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
// `table` prefixes the view fields, and `fields` names the column each header sorts by, in header order.
function setupSort(table, tableId, fields) {
    var tableElement = $(tableId);
    if (!tableElement) return;
    var headerRow = tableElement.querySelector("thead tr");
    if (!headerRow) return;

    var ths = headerRow.children;
    var sortField = saved.view[table + "_sort"] || null;
    var ascending = saved.view[table + "_sort_ascending"] !== false;
    for (var i = 0; i < ths.length && i < fields.length; i++) {
        ths[i].classList.add("sortable");
        ths[i].setAttribute("data-sort-field", fields[i]);
        if (fields[i] === sortField) ths[i].classList.add(ascending ? "sort-asc" : "sort-desc");
        (function(th, field) {
            th.addEventListener("click", function() {
                ascending = sortField === field ? !ascending : true;
                sortField = field;
                for (var k = 0; k < ths.length; k++) ths[k].classList.remove("sort-asc", "sort-desc");
                th.classList.add(ascending ? "sort-asc" : "sort-desc");
                var change = {};
                change[table + "_sort"] = field;
                change[table + "_sort_ascending"] = ascending;
                change[table + "_page"] = 0;
                sendView(change);
            });
        })(ths[i], fields[i]);
    }
}

setupSort("workers", "workers-table", WORKER_FIELDS);
setupSort("task_log", "tasklog-table", TASK_LOG_FIELDS);
setupSort("task_events", "taskevents-table", TASK_EVENT_FIELDS);
setupSort("objects", "objects-table", OBJECT_FIELDS);

// Columns drawn as a bar: a number is the fixed maximum, a string names the row field to divide by.
var WORKER_GAUGE_FIELDS = {
    "agt_cpu": {max: 100, unit: "%"},
    "agt_rss": {max: "total_rss", unit: ""},
    "proc_cpu": {max: 100, unit: "%"},
    "proc_rss": {max: "total_rss", unit: ""},
    "mem_used_pct": {max: 100, unit: "%"}
};

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

// Gauge as an HTML string, for the tables that are rebuilt wholesale.
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

// Cells are filled from WORKER_FIELDS, the list the row was built from, so column order lives in one place.
function updateWorkerRow(tr, w) {
    for (var i = 0; i < WORKER_FIELDS.length; i++) {
        var field = WORKER_FIELDS[i];
        var gauge = WORKER_GAUGE_FIELDS[field];
        if (gauge) {
            setGauge(tr.children[i], w[field], typeof gauge.max === "string" ? w[gauge.max] : gauge.max, gauge.unit);
        } else {
            tr.children[i].textContent = (w[field] === undefined || w[field] === null || w[field] === "")
                ? "—" : w[field];
        }
    }
    workerCell(tr, "name").title = w.full_name || w.name;
    workerCell(tr, "host").title = w.host;
    workerCell(tr, "mem_used_pct").title = w.mem_limit ? (w.mem_used + " / " + w.mem_limit + " MB used") : "";
}

function workerCell(tr, field) {
    return tr.querySelector('[data-field="' + field + '"]');
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

function updateTaskLog(rows) {
    taskLogData = rows;
    if (activeTab === "tasklist") renderTaskLog();
    else updateTaskLogBadge();  // the badge (server total) stays current even while the tab is hidden
}

function renderTaskLog() {
    tasklogBody.innerHTML = "";
    for (var i = 0; i < taskLogData.length; i++) tasklogBody.appendChild(makeTaskLogRow(taskLogData[i]));
    updateTaskLogBadge();
    renderPagers("tasklog-pager", taskLogPage, taskLogPages, taskLogHeld, function(p) {
        taskLogPage = p;
        sendView({ task_log_page: p });
    });
}

function makeCell(text) {
    var td = document.createElement("td");
    td.textContent = text == null ? "" : text;
    return td;
}

// Cells that carry more than the row's own text; every other column is its value.
var TASK_LOG_CELLS = {
    task_id: function(e) {
        var td = document.createElement("td");
        var span = document.createElement("span");
        span.className = "task-id";
        span.textContent = e.task_id;
        span.title = e.task_id;
        span.addEventListener("click", function() {
            if (navigator.clipboard) navigator.clipboard.writeText(e.task_id);
        });
        td.appendChild(span);
        return td;
    },
    client: function(e) {
        var td = makeCell(e.client || "\u2014");
        td.title = e.full_client || e.client || "";
        return td;
    },
    worker: function(e) {
        var td = makeCell(e.worker || "");
        td.title = e.full_worker || e.worker || "";
        return td;
    },
    time: function(e) { return makeCell(formatTime(e.time)); },
    status: function(e) {
        var td = makeCell(e.status);
        td.className = statusClass(e.status);
        return td;
    }
};

function makeTaskLogRow(e) {
    var tr = document.createElement("tr");
    tr.dataset.taskId = e.task_id;
    for (var i = 0; i < TASK_LOG_FIELDS.length; i++) {
        var field = TASK_LOG_FIELDS[i];
        tr.appendChild(TASK_LOG_CELLS[field] ? TASK_LOG_CELLS[field](e) : makeCell(e[field]));
    }
    return tr;
}

// Badge counts every completed task, and once the server drops the oldest, "60123 (holding 50000)".
function updateTaskLogBadge() {
    tasklogCount.textContent = taskLogTotal > taskLogHeld
        ? taskLogTotal + " (holding " + taskLogHeld + ")"
        : taskLogTotal;
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

// -- Memory and CPU Chart (Canvas) --
var MEM_LABEL_WIDTH = 80;
var CPU_LABEL_WIDTH = 60;
var CPU_COLOR = "#d97706";
var MEM_PADDING = { top: 20, right: CPU_LABEL_WIDTH, bottom: 30, left: MEM_LABEL_WIDTH };

function updateMemoryChart(data) {
    memoryPoints = data.points || [];
    memoryYTicks = data.y_ticks || [];
    lastCpuTicks = data.cpu_ticks || [];
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
        memoryCtx.fillText("No samples yet", cw / 2, ch / 2);
        return;
    }

    // Determine y range
    var maxY = 0;
    for (var i = 0; i < memoryPoints.length; i++) {
        if (memoryPoints[i].y > maxY) maxY = memoryPoints[i].y;
    }
    maxY = Math.max(maxY, 1024 * 1024 * 1024); // min 1GB
    var cpuMax = lastCpuTicks.length ? lastCpuTicks[lastCpuTicks.length - 1].val : 0;

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

    function mapCpuY(val) {
        return plotTop + plotHeight - (val / cpuMax) * plotHeight;
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

    // CPU reads off the right edge, in ticks spaced like the memory grid, so a linear scale shares its lines.
    memoryCtx.textAlign = "left";
    memoryCtx.strokeStyle = CPU_COLOR;
    memoryCtx.fillStyle = CPU_COLOR;
    for (var u = 0; u < lastCpuTicks.length; u++) {
        var uy = mapCpuY(lastCpuTicks[u].val);
        memoryCtx.beginPath();
        memoryCtx.moveTo(plotLeft + plotWidth, uy);
        memoryCtx.lineTo(plotLeft + plotWidth + 4, uy);
        memoryCtx.stroke();
        memoryCtx.fillText(lastCpuTicks[u].label, plotLeft + plotWidth + 7, uy);
    }
    memoryCtx.strokeStyle = "#e2e8f0";
    memoryCtx.fillStyle = "#64748b";

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

    // CPU against the right axis: the shape says whether held memory is computing.
    if (lastCpuPoints.length > 1 && cpuMax > 0) {
        memoryCtx.beginPath();
        for (var k = 0; k < lastCpuPoints.length; k++) {
            var cx = mapX(lastCpuPoints[k].x);
            var cy = mapCpuY(lastCpuPoints[k].y);
            if (k === 0) memoryCtx.moveTo(cx, cy);
            else memoryCtx.lineTo(cx, cy);
        }
        memoryCtx.strokeStyle = CPU_COLOR;
        memoryCtx.lineWidth = 1.5;
        memoryCtx.setLineDash([4, 3]);
        memoryCtx.stroke();
        memoryCtx.setLineDash([]);
    }

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

    var closest = closestPoint(memoryPoints, t);
    if (closest && Math.abs(closest.x - t) < streamWindow * 0.05) {
        var cpu = closestPoint(lastCpuPoints, closest.x);
        var reading = formatBytes(closest.y) + (cpu ? ", CPU " + cpu.y + "%" : "");
        tooltip.textContent = reading + " at " + closest.x.toFixed(1) + "s";
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

// The point of `points` nearest time `x`, or null when there are none.
function closestPoint(points, x) {
    var closest = null;
    for (var i = 0; i < points.length; i++) {
        if (closest === null || Math.abs(points[i].x - x) < Math.abs(closest.x - x)) closest = points[i];
    }
    return closest;
}

// -- Workers --
var managerCollapsed = {};  // manager id -> folded by this browser
// A rebuild between a press and its release swallows the click, so the rows hold still briefly after a press.
var CLICK_HOLD_MS = 600;
var workersPressedAt = 0;

workerDetailsContainer.addEventListener("pointerdown", function() { workersPressedAt = Date.now(); });

function updateWorkerDetails(workerDetails) {
    lastWorkerDetails = workerDetails;
    if (activeTab === "workers") renderWorkerDetails();
}

function renderWorkerDetails() {
    if (Date.now() - workersPressedAt < CLICK_HOLD_MS) return;  // the next update draws it

    // Each group carries fleet-wide summary numbers, but only this page's workers.
    var groups = lastWorkerDetails || [];
    workerDetailsContainer.innerHTML = "";
    workerDetailsCount.textContent = workerDetailsTotal ? "(" + workerDetailsTotal + ")" : "";
    if (workerDetailsTotal === 0) {
        workerDetailsContainer.appendChild(makeElement("div", "worker-empty worker-detail", "No workers connected"));
    }
    for (var g = 0; g < groups.length; g++) {
        if (groups[g].workers && groups[g].workers.length > 0) {
            workerDetailsContainer.appendChild(buildWorkerGroup(groups[g]));
        }
    }
    renderPagers("workerdetails-pager", workerDetailsPage, workerDetailsPages, workerDetailsTotal, function(p) {
        workerDetailsPage = p;
        sendView({ worker_details_page: p });
    });
}

// A manager's workers under a row that sums the same four columns over every worker the manager has.
function buildWorkerGroup(group) {
    var details = makeElement("details", "worker-group");
    details.open = !managerCollapsed[group.manager_id];
    details.addEventListener("toggle", function() { managerCollapsed[group.manager_id] = !details.open; });

    var name = group.manager_id === "\u2014" ? "No manager" : group.manager_id;
    var count = group.worker_count + (group.worker_count === 1 ? " worker" : " workers");
    var manager = makeElement("div");
    manager.appendChild(makeElement("span", "worker-group-name", name));
    manager.appendChild(document.createTextNode(" " + count));

    var busy = group.active_processors + " of " + group.total_processors + " processors busy";
    var summary = makeElement("summary", "worker-row worker-group-head");
    summary.appendChild(manager);
    summary.appendChild(makeElement("div", "", group.total_rss + " MB \u00b7 " + group.total_cpu + "% CPU"));
    summary.appendChild(makeElement("div", "", busy, "Processors holding a task, of every one these workers run"));
    summary.appendChild(makeElement("div", "", group.total_queued + " queued", "As the workers themselves report it"));
    details.appendChild(summary);

    for (var i = 0; i < group.workers.length; i++) details.appendChild(buildWorkerRow(group.workers[i]));
    return details;
}

function buildWorkerRow(worker) {
    var row = makeElement("div", "worker-row");
    row.appendChild(buildWorkerIdentity(worker));
    row.appendChild(buildWorkerResources(worker));
    row.appendChild(buildWorkerRunning(worker));
    row.appendChild(buildWorkerQueue(worker));
    return row;
}

function buildWorkerIdentity(worker) {
    var cell = makeElement("div");
    cell.appendChild(makeElement("div", "worker-name", worker.name, worker.full_name || worker.name));
    cell.appendChild(makeElement("div", "worker-detail", worker.host, worker.host));
    cell.appendChild(makeElement("div", "worker-detail", "seen " + worker.last_seen + " ago", "Its last heartbeat"));
    if (worker.capabilities && worker.capabilities !== "<no capabilities>") {
        cell.appendChild(makeElement("div", "worker-detail", worker.capabilities, "Capabilities"));
    }
    return cell;
}

function buildWorkerResources(worker) {
    var limit = worker.mem_limit ? "Of the " + worker.mem_limit + " MB limit this worker runs under" : "";
    var cell = makeElement("div");
    cell.appendChild(buildMeter("Mem", worker.mem_used_pct, limit));
    cell.appendChild(buildMeter("CPU", worker.cpu, "Where one core is 100%"));
    var pss = makeElement("div", "meter", null, "PSS on Linux, RSS on macOS and Windows");
    pss.appendChild(makeElement("span", "meter-label", "PSS"));
    pss.appendChild(makeElement("span", "", worker.rss + " MB"));
    cell.appendChild(pss);
    return cell;
}

// A labelled percentage gauge.
function buildMeter(label, percent, title) {
    var meter = makeElement("div", "meter", null, title);
    meter.appendChild(makeElement("span", "meter-label", label));
    meter.insertAdjacentHTML("beforeend", makeGaugeHTML(percent, 100, "%"));
    return meter;
}

// One entry per processor: the task it holds and for how long, then the process itself.
function buildWorkerRunning(worker) {
    var cell = makeElement("div");
    for (var p = 0; p < worker.processors.length; p++) cell.appendChild(buildProcessor(worker.processors[p]));
    return cell;
}

function buildProcessor(proc) {
    var task = makeElement("div", "processor-task");
    if (proc.task_id) {
        if (proc.function) task.appendChild(makeElement("span", "processor-function", proc.function, proc.function));
        task.appendChild(makeTaskLink(proc.task_id, "span"));
        task.appendChild(makeElement("span", "worker-detail", proc.task_age, "How long it has held the task"));
    } else {
        task.appendChild(makeElement("span", "worker-detail", "idle"));
    }
    if (proc.suspended) {
        task.appendChild(makeElement("span", "tag tag-suspended", "suspended", "Holding a task without running it"));
    }
    if (!proc.initialized) {
        task.appendChild(makeElement("span", "tag", "starting", "Has not loaded the client's environment yet"));
    }

    var memory = proc.rss + " MB, peak " + proc.peak_rss + " MB";
    var usage = ["pid " + proc.pid, proc.cpu + "% CPU", memory].join(" \u00b7 ");
    var processor = makeElement("div", "processor");
    processor.appendChild(task);
    processor.appendChild(makeElement("div", "worker-detail", usage, "The peak is the highest this monitor has seen"));
    return processor;
}

// What this worker holds and has not started, next in line first.
function buildWorkerQueue(worker) {
    var depth = Math.max(worker.queue_depth, worker.queue_named);
    var head = makeElement("div", "queue-head");
    head.appendChild(makeElement("span", depth > 0 ? "queue-count" : "worker-detail", depth + " queued"));
    head.appendChild(makeElement("span", "worker-detail", " \u00b7 " + worker.free + " free", "Queue slots left"));
    head.appendChild(makeElement("span", "worker-detail", " \u00b7 " + worker.sent + " sent", "Not yet answered"));

    var cell = makeElement("div");
    cell.appendChild(head);
    if (worker.queue.length === 0) {
        if (depth > 0) cell.appendChild(makeElement("div", "worker-detail", "queued before this monitor started"));
        return cell;
    }

    var chips = makeElement("div", "queue-chips");
    for (var i = 0; i < worker.queue.length; i++) chips.appendChild(buildQueueChip(worker.queue[i]));
    if (depth > worker.queue.length) {
        chips.appendChild(makeElement("span", "chip chip-more", "+" + (depth - worker.queue.length), "Further back"));
    }
    cell.appendChild(chips);
    return cell;
}

function buildQueueChip(task) {
    var label = task.function || task.task_id.slice(0, 12);
    var chip = makeElement("span", "chip task-link", label, task.task_id + " - click for this task's trail");
    chip.addEventListener("click", function() { focusTask(task.task_id); });
    return chip;
}

// A task id that opens that task's trail in the Task Log.
function makeTaskLink(taskId, tag) {
    var element = document.createElement(tag || "td");
    var link = document.createElement("span");
    link.className = "task-id task-link";
    link.textContent = taskId.slice(0, 12);
    link.title = taskId + " - click for this task's trail";
    link.addEventListener("click", function(evt) {
        evt.preventDefault();
        evt.stopPropagation();
        focusTask(taskId);
    });
    element.appendChild(link);
    return element;
}

// Show one task's trail: switch to the Task Log and filter it to that task.
function focusTask(taskId) {
    selectTab("tasklog");
    showOnlyTask(taskId);
}

// An element with the class, text and tooltip most cells set.
function makeElement(tag, className, text, title) {
    var node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined && text !== null) node.textContent = text;
    if (title) node.title = title;
    return node;
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
applySettings(saved.settings);
selectTab($("panel-" + saved.tab) ? saved.tab : "live");
connect();
requestAnimationFrame(renderLoop);
