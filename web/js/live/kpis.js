// KPI tiles (DESIGN.md §4, §7 stat tile): Empty now · Full now · Open alerts · Bikes available, for all regions.
// Values come straight from live.json (`counts.all`, `bikes_available`, `station_count`) and alerts.json (`open`).

const number = new Intl.NumberFormat("en-US");

export function createKpis(section) {
  const tiles = Object.fromEntries([...section.querySelectorAll("[data-kpi]")].map((t) => [t.dataset.kpi, t]));

  function set(key, value, sub) {
    const tile = tiles[key];
    tile.querySelector("[data-kpi-value]").textContent = value;
    tile.querySelector("[data-kpi-sub]").textContent = sub;
  }

  function render(live, alerts) {
    const all = live.counts?.all ?? {};
    const stations = number.format(live.station_count ?? live.stations?.length ?? 0);
    set("empty", number.format(all.empty ?? 0), `of ${stations} stations`);
    set("full", number.format(all.full ?? 0), `of ${stations} stations`);
    if (alerts && Array.isArray(alerts.open)) {
      set("alerts", number.format(alerts.open.length), "stations empty or full for 15 min or more");
    } else if (alerts === false) {
      set("alerts", "—", "Alerts couldn’t be loaded");
    }
    const ebikes = (live.stations ?? []).reduce((sum, s) => sum + (s.ebikes ?? 0), 0);
    set("bikes", number.format(live.bikes_available ?? 0), `including ${number.format(ebikes)} e-bikes`);
    section.removeAttribute("aria-busy");
  }

  // ⓘ buttons reveal each tile's definition.
  section.addEventListener("click", (event) => {
    const button = event.target.closest("[data-kpi-info]");
    if (!button) return;
    const definition = document.getElementById(button.getAttribute("aria-controls"));
    const open = button.getAttribute("aria-expanded") !== "true";
    button.setAttribute("aria-expanded", String(open));
    definition.hidden = !open;
  });

  return { render };
}
