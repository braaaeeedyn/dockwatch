// Live page entry: the shared shell, then the live station map (F2) in #map.

import { initLive } from "./live/live.js";
import { initShell } from "./shell.js";

initShell();
initLive();
