(function () {
  const log = document.getElementById("log");
  const state = document.getElementById("state");
  const MAX_ROWS = 400;

  function post(url, body) {
    return fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: body ? JSON.stringify(body) : null,
    });
  }

  function bindActions(root) {
    root.querySelectorAll("button.act").forEach(function (btn) {
      if (btn.dataset.bound) return;
      btn.dataset.bound = "1";
      btn.addEventListener("click", function () {
        const confirmMsg = btn.dataset.confirm;
        if (confirmMsg && !window.confirm(confirmMsg)) return;
        post(btn.dataset.action);
      });
    });

    root.querySelectorAll("input.drop").forEach(function (el) {
      if (el.dataset.bound) return;
      el.dataset.bound = "1";
      const out = document.getElementById(el.id + "-out");
      el.addEventListener("input", function () {
        if (out) out.textContent = el.value + "%";
      });
      el.addEventListener("change", function () {
        post("/api/faults/drop", {
          prepare: pct("drop-prepare"),
          accept: pct("drop-accept"),
        });
      });
    });

    const isolate = root.querySelector("#isolate");
    if (isolate && !isolate.dataset.bound) {
      isolate.dataset.bound = "1";
      isolate.addEventListener("change", function () {
        post("/api/faults/isolate", { isolated: isolate.checked });
      });
    }

    const apply = root.querySelector("#latency-apply");
    if (apply && !apply.dataset.bound) {
      apply.dataset.bound = "1";
      apply.addEventListener("click", function () {
        post("/api/faults/latency", {
          min_ms: num("latency-min"),
          max_ms: num("latency-max"),
        });
      });
    }

    root.querySelectorAll("input.peer-block").forEach(function (el) {
      if (el.dataset.bound) return;
      el.dataset.bound = "1";
      el.addEventListener("change", function () {
        post("/api/faults/peer", {
          peer_id: parseInt(el.dataset.peer, 10),
          blocked: el.checked,
        });
      });
    });
  }

  function pct(id) {
    const el = document.getElementById(id);
    return el ? parseInt(el.value, 10) / 100 : 0;
  }

  function num(id) {
    const el = document.getElementById(id);
    return el ? parseInt(el.value, 10) || 0 : 0;
  }

  const clear = document.getElementById("clear-log");
  if (clear) {
    clear.addEventListener("click", function () {
      log.innerHTML = "";
    });
  }

  const source = new EventSource("/api/stream");

  source.addEventListener("state", function (e) {
    state.innerHTML = JSON.parse(e.data);
    bindActions(document);
  });

  source.addEventListener("chrome", function (e) {
    const parsed = JSON.parse(e.data);
    document.getElementById("header-badges").innerHTML = parsed.badges;
    const panel = document.getElementById("controls-panel");
    if (panel && parsed.controls) panel.innerHTML = parsed.controls;
    bindActions(document);
  });

  source.addEventListener("log", function (e) {
    const row = document.createElement("div");
    row.innerHTML = JSON.parse(e.data);
    const node = row.firstElementChild;
    if (!node) return;
    log.insertBefore(node, log.firstChild);
    while (log.childElementCount > MAX_ROWS) {
      log.removeChild(log.lastElementChild);
    }
  });

  bindActions(document);
})();
