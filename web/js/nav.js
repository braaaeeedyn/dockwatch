// Below 1120 px the nav links live in a full-screen <dialog>: focus trapped, Esc closes, page scroll locked,
// focus returns to the menu button on close.

const DESKTOP = window.matchMedia("(min-width: 1120px)");
const FOCUSABLE = 'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), [tabindex]:not([tabindex="-1"])';

export function initNav() {
  const button = document.querySelector("[data-nav-open]");
  const menu = document.getElementById("nav-menu");
  if (!button || !menu || typeof menu.showModal !== "function") return;

  const focusables = () => [...menu.querySelectorAll(FOCUSABLE)].filter((el) => el.offsetParent !== null);

  function open() {
    menu.showModal();
    document.documentElement.classList.add("is-scroll-locked");
    button.setAttribute("aria-expanded", "true");
    const close = menu.querySelector("[data-nav-close]");
    (close || focusables()[0])?.focus();
  }

  function close() {
    if (menu.open) menu.close(); // the "close" event below does the cleanup
  }

  menu.addEventListener("close", () => {
    document.documentElement.classList.remove("is-scroll-locked");
    button.setAttribute("aria-expanded", "false");
    button.focus();
  });

  // Esc: the dialog's own "cancel" also fires, but handle it here so behaviour is the same in every browser.
  menu.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      event.preventDefault();
      close();
      return;
    }
    if (event.key !== "Tab") return;
    const items = focusables();
    if (items.length === 0) return;
    const first = items[0];
    const last = items[items.length - 1];
    const active = document.activeElement;
    if (event.shiftKey && (active === first || !menu.contains(active))) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && (active === last || !menu.contains(active))) {
      event.preventDefault();
      first.focus();
    }
  });

  button.addEventListener("click", open);
  menu.querySelector("[data-nav-close]")?.addEventListener("click", close);
  // Same-page links (and the wordmark) should still close the overlay.
  for (const link of menu.querySelectorAll("a[href]")) link.addEventListener("click", close);
  DESKTOP.addEventListener("change", (event) => {
    if (event.matches) close();
  });
}
