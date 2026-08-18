(function () {
  const steps = document.getElementById("steps");
  const grid = document.getElementById("grid");
  const summary = document.getElementById("summary");
  const select = document.getElementById("scenario");
  const MAX_ROWS = 500;

  function post(url, body) {
    return fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: body ? JSON.stringify(body) : null,
    }).then(function (res) {
      if (!res.ok && res.status !== 204 && res.status !== 202) {
        return res.text().then(function (t) {
          window.alert(t || res.statusText);
        });
      }
    });
  }

  function showParams() {
    const active = select.value;
    document.querySelectorAll(".scenario-params").forEach(function (el) {
      el.hidden = el.dataset.scenario !== active;
    });
  }

  select.addEventListener("change", showParams);
  showParams();

  document.getElementById("run").addEventListener("click", function () {
    const active = select.value;
    const panel = document.querySelector('.scenario-params[data-scenario="' + active + '"]');
    const params = {};
    if (panel) {
      panel.querySelectorAll("input.param").forEach(function (el) {
        params[el.dataset.param] = el.type === "checkbox" ? String(el.checked) : el.value;
      });
    }
    post("/api/scenario/run", { scenario: active, params: params });
  });

  document.getElementById("stop").addEventListener("click", function () {
    post("/api/scenario/stop");
  });

  const tabs = Array.prototype.slice.call(document.querySelectorAll("button.logtab"));
  const panes = Array.prototype.slice.call(document.querySelectorAll(".logpane"));
  const ACTIVE = ["bg-slate-800", "text-slate-100", "ring-1", "ring-slate-700"];
  const IDLE = ["text-slate-500", "hover:text-slate-300"];

  function selectTab(name) {
    tabs.forEach(function (tab) {
      const on = tab.dataset.tab === name;
      ACTIVE.forEach(function (c) { tab.classList.toggle(c, on); });
      IDLE.forEach(function (c) { tab.classList.toggle(c, !on); });
    });
    panes.forEach(function (pane) {
      pane.hidden = pane.dataset.pane !== name;
    });
  }

  tabs.forEach(function (tab) {
    tab.addEventListener("click", function () {
      selectTab(tab.dataset.tab);
    });
  });
  selectTab("run");

  document.getElementById("clear-steps").addEventListener("click", function () {
    panes.forEach(function (pane) {
      pane.innerHTML = "";
    });
  });

  function appendRow(pane, html) {
    if (!pane) return;
    const row = document.createElement("div");
    row.innerHTML = html;
    const node = row.firstElementChild;
    if (!node) return;
    pane.insertBefore(node, pane.firstChild);
    while (pane.childElementCount > MAX_ROWS) {
      pane.removeChild(pane.lastElementChild);
    }
  }

  document.querySelectorAll("button.cact").forEach(function (btn) {
    btn.addEventListener("click", function () {
      const msg = btn.dataset.confirm;
      if (msg && !window.confirm(msg)) return;
      post("/api/cluster/" + btn.dataset.cluster);
    });
  });

  const source = new EventSource("/api/stream");

  source.addEventListener("grid", function (e) {
    const parsed = JSON.parse(e.data);
    grid.innerHTML = parsed.grid;
    summary.innerHTML = parsed.summary;
  });

  source.addEventListener("step", function (e) {
    appendRow(steps, JSON.parse(e.data));
  });

  source.addEventListener("nodelog", function (e) {
    const parsed = JSON.parse(e.data);
    appendRow(document.getElementById("nodelog-" + parsed.node), parsed.html);
  });
})();
