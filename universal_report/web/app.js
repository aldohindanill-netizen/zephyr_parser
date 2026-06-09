const STATUS_OPTIONS = [
  "Pass",
  "Fail",
  "Not Executed",
  "In Progress",
  "Blocked",
  "Can't Test",
  "Not Tested PI",
  "Danger",
  "Can't Reproduce",
  "False Positive",
];

let draft = null;
let currentStep = 1;
const MAX_STEP = 4;
let selectedFolder = { id: "", name: "", fullPath: "" };
const selectedCycles = new Map();
let folderTreeData = [];
let folderTreeLoading = false;
let folderCyclesByFolder = {};
const folderCyclesFetched = new Set();
const folderCyclesLoading = new Set();
const folderCyclesLoadPromises = new Map();
let zephyrImportLoading = false;
const folderTreeExpanded = new Set();
const folderTreeFilterExpanded = new Set();

const homeView = document.getElementById("home-view");
const editorView = document.getElementById("editor-view");
const draftsBody = document.getElementById("drafts-body");
const statusBox = document.getElementById("status-box");

function showStatus(message, isError = false) {
  statusBox.textContent = message;
  statusBox.classList.remove("hidden", "ok", "err");
  statusBox.classList.add(isError ? "err" : "ok");
}

function clearStatus() {
  statusBox.classList.add("hidden");
  statusBox.textContent = "";
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const err = data.error || `HTTP ${response.status}`;
    throw new Error(data.trace ? `${err}\n${data.trace}` : err);
  }
  return data;
}

function syncDraftFromForm() {
  if (!draft) return;
  draft.meta.title = document.getElementById("meta-title").value.trim();
  draft.meta.report_date = document.getElementById("meta-date").value;
  draft.meta.build_name = document.getElementById("meta-build").value.trim();
  draft.meta.folder_name = draft.meta.build_name;

  const s12 = draft.sections_1_2;
  s12.object_description_prefix = document.getElementById("s12-prefix").value.trim();
  s12.build_name = document.getElementById("s12-build").value.trim();
  s12.speed_kmh = Number(document.getElementById("s12-speed").value || 40);

  s12.document_links = [...document.querySelectorAll("#doc-links .doc-link-row")].map((row) => ({
    label: row.querySelector(".doc-label").value.trim(),
    url: row.querySelector(".doc-url").value.trim(),
    note: row.querySelector(".doc-note").value.trim(),
  }));

  s12.infrastructure = [...document.querySelectorAll("#infra-list .chip-row input")].map((el) =>
    el.value.trim()
  ).filter(Boolean);

  s12.equipment = [...document.querySelectorAll("#equip-list .chip-row input")].map((el) =>
    el.value.trim()
  ).filter(Boolean);

  if (!isZephyrImportedView()) {
    draft.cycles = collectCyclesFromDom();
  }
}

function fillFormFromDraft() {
  if (!draft) return;
  document.getElementById("meta-title").value = draft.meta.title || "";
  document.getElementById("meta-date").value = draft.meta.report_date || "";
  document.getElementById("meta-build").value = draft.meta.build_name || "";

  const s12 = draft.sections_1_2;
  document.getElementById("s12-prefix").value = s12.object_description_prefix || "";
  document.getElementById("s12-build").value = s12.build_name || "";
  document.getElementById("s12-speed").value = s12.speed_kmh ?? 40;

  renderDocLinks(s12.document_links || []);
  renderChipList("infra-list", s12.infrastructure || []);
  renderChipList("equip-list", s12.equipment || []);
  setSection3Mode(draft.section_3_mode || "manual");
  updateStep3CyclesView();
  updateSelectedFolderLabel();
}

function renderDocLinks(links) {
  const root = document.getElementById("doc-links");
  root.innerHTML = "";
  (links.length ? links : [{ label: "", url: "", note: "" }]).forEach((link, idx) => {
    const row = document.createElement("div");
    row.className = "doc-link-row";
    row.innerHTML = `
      <input class="doc-label" type="text" placeholder="Название" value="${escapeAttr(link.label || "")}">
      <input class="doc-url" type="text" placeholder="URL" value="${escapeAttr(link.url || "")}">
      <input class="doc-note" type="text" placeholder="Примечание" value="${escapeAttr(link.note || "")}">
      <button type="button" class="danger" data-remove-doc="${idx}">×</button>
    `;
    row.querySelector("button").addEventListener("click", () => {
      syncDraftFromForm();
      const next = draft.sections_1_2.document_links.filter((_, i) => i !== idx);
      renderDocLinks(next);
    });
    root.appendChild(row);
  });
}

function renderChipList(containerId, items) {
  const root = document.getElementById(containerId);
  root.innerHTML = "";
  (items.length ? items : [""]).forEach((value, idx) => {
    const row = document.createElement("div");
    row.className = "chip-row";
    row.innerHTML = `
      <input type="text" value="${escapeAttr(value)}">
      <button type="button" class="danger">×</button>
    `;
    row.querySelector("button").addEventListener("click", () => {
      syncDraftFromForm();
      const key = containerId === "infra-list" ? "infrastructure" : "equipment";
      const next = draft.sections_1_2[key].filter((_, i) => i !== idx);
      renderChipList(containerId, next.length ? next : [""]);
    });
    root.appendChild(row);
  });
}

function escapeAttr(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/"/g, "&quot;")
    .replace(/</g, "&lt;");
}

function isZephyrImportedView() {
  return draft?.section_3_mode === "zephyr" && (draft?.cycles?.length || 0) > 0;
}

function cycleNodeKey(node) {
  return node.testRunId || node.cycleKey || node.id;
}

function getFolderDisplayName(folderId, fallbackName = "") {
  const folder = folderTreeData.find((item) => item.id === folderId);
  return folder?.full_path || folder?.name || fallbackName || folderId;
}

function getActiveFolderLoadingLabel() {
  if (zephyrImportLoading && selectedFolder.id) {
    return getFolderDisplayName(selectedFolder.id, selectedFolder.name);
  }
  const loadingId = [...folderCyclesLoading][0];
  if (!loadingId) {
    return "";
  }
  return getFolderDisplayName(loadingId);
}

function updateZephyrLoadingIndicator() {
  const box = document.getElementById("zephyr-tree-loading");
  const text = document.getElementById("zephyr-tree-loading-text");
  const overlay = document.getElementById("zephyr-folder-tree-overlay");
  const overlayText = document.getElementById("zephyr-folder-tree-overlay-text");
  const importBtn = document.getElementById("btn-import-zephyr");
  const loadingFolder = getActiveFolderLoadingLabel();
  let bannerMessage = "";
  let overlayMessage = "";
  let showBanner = false;
  let showOverlay = false;

  if (folderTreeLoading) {
    showBanner = true;
    bannerMessage = "Загрузка дерева папок Zephyr...";
    showOverlay = true;
    overlayMessage = "Загрузка папок...";
  } else if (zephyrImportLoading) {
    showBanner = true;
    bannerMessage = loadingFolder
      ? `Импорт кейсов из папки «${loadingFolder}»...`
      : "Импорт кейсов из Zephyr...";
    showOverlay = true;
    overlayMessage = "Загрузка кейсов из Zephyr...";
  } else if (folderCyclesLoading.size > 0) {
    showBanner = true;
    bannerMessage = loadingFolder
      ? `Загрузка тест-циклов: «${loadingFolder}»...`
      : "Загрузка тест-циклов...";
    showOverlay = true;
    overlayMessage = "Загрузка циклов в папке...";
  }

  if (box && text) {
    box.classList.toggle("hidden", !showBanner);
    if (showBanner) {
      text.textContent = bannerMessage;
    }
  }
  if (overlay && overlayText) {
    overlay.classList.toggle("hidden", !showOverlay);
    if (showOverlay) {
      overlayText.textContent = overlayMessage;
    }
  }
  if (importBtn) {
    importBtn.disabled = zephyrImportLoading || folderTreeLoading;
    importBtn.classList.toggle("is-loading", zephyrImportLoading);
    if (zephyrImportLoading) {
      if (!importBtn.dataset.originalText) {
        importBtn.dataset.originalText = importBtn.textContent;
      }
      importBtn.textContent = "Загрузка из Zephyr...";
    } else if (importBtn.dataset.originalText) {
      importBtn.textContent = importBtn.dataset.originalText;
    }
  }
}

function renderCycles(cycles) {
  const root = document.getElementById("cycles-root");
  root.innerHTML = "";
  cycles.forEach((cycle, cycleIdx) => {
    root.appendChild(buildCycleBlock(cycle, cycleIdx));
  });
}

function renderCyclesSummary(cycles) {
  const root = document.getElementById("cycles-root");
  root.innerHTML = "";
  const cycleCount = cycles.length;
  const caseCount = cycles.reduce((sum, cycle) => sum + (cycle.cases?.length || 0), 0);
  const summary = document.createElement("div");
  summary.className = "cycles-import-summary";
  summary.innerHTML = `
    <p>Импортировано из Zephyr: <strong>${cycleCount}</strong> циклов, <strong>${caseCount}</strong> кейсов.</p>
    <p class="folder-tree-hint">Кейсы попадут в отчёт автоматически. Редактирование таблиц не требуется.</p>
  `;
  const list = document.createElement("ul");
  list.className = "cycles-import-list";
  cycles.forEach((cycle) => {
    const item = document.createElement("li");
    const title = [cycle.cycle_key, cycle.cycle_name].filter(Boolean).join(" — ") || cycle.cycle_id;
    const cases = cycle.cases?.length || 0;
    item.textContent = `${title} (${cases} кейсов)`;
    list.appendChild(item);
  });
  summary.appendChild(list);
  root.appendChild(summary);
}

function updateStep3CyclesView() {
  const addBtn = document.getElementById("btn-add-cycle");
  if (!draft) {
    return;
  }
  if (isZephyrImportedView()) {
    addBtn.classList.add("hidden");
    renderCyclesSummary(draft.cycles || []);
    return;
  }
  addBtn.classList.remove("hidden");
  renderCycles(draft.cycles || []);
}

function buildCycleBlock(cycle, cycleIdx) {
  const block = document.createElement("div");
  block.className = "cycle-block";
  block.dataset.cycleIdx = String(cycleIdx);
  block.dataset.cycleId = cycle.cycle_id || `cycle-${cycleIdx + 1}`;
  block.dataset.cycleObjective = cycle.cycle_objective || "";
  block.innerHTML = `
    <h3>Тестовый цикл ${cycleIdx + 1}</h3>
    <div class="grid-2">
      <div><label>Ключ цикла</label><input class="cycle-key" type="text" value="${escapeAttr(cycle.cycle_key || "")}"></div>
      <div><label>Название цикла</label><input class="cycle-name" type="text" value="${escapeAttr(cycle.cycle_name || "")}"></div>
    </div>
    <div class="btn-row">
      <button type="button" class="btn-add-case">Добавить тест-кейс</button>
      <button type="button" class="btn-remove-cycle danger">Удалить цикл</button>
    </div>
    <table class="case-table">
      <thead>
        <tr>
          <th>Название</th>
          <th>Критерий</th>
          <th>Статус</th>
          <th>Комментарий</th>
          <th>Задачи Jira</th>
          <th></th>
        </tr>
      </thead>
      <tbody class="case-body"></tbody>
    </table>
  `;

  const tbody = block.querySelector(".case-body");
  (cycle.cases || []).forEach((testCase, caseIdx) => {
    tbody.appendChild(buildCaseRow(testCase, caseIdx));
  });

  block.querySelector(".btn-add-case").addEventListener("click", () => {
    syncDraftFromForm();
    draft.cycles[cycleIdx].cases.push({
      test_case_key: "",
      test_case_name: "",
      result: "Pass",
      objective: "",
      comment: "",
      tasks: "",
    });
    renderCycles(draft.cycles);
  });

  block.querySelector(".btn-remove-cycle").addEventListener("click", () => {
    syncDraftFromForm();
    draft.cycles.splice(cycleIdx, 1);
    renderCycles(draft.cycles);
  });

  return block;
}

function buildCaseRow(testCase, caseIdx) {
  const row = document.createElement("tr");
  row.dataset.caseIdx = String(caseIdx);
  row.dataset.testCaseKey = testCase.test_case_key || "";
  row.dataset.executionDate = testCase.execution_date || "";
  row.dataset.actualStartDate = testCase.actual_start_date || "";
  row.dataset.caseIterationKey = testCase.case_iteration_key || "";
  row.dataset.logsSourceText = testCase.logs_source_text || "";
  const options = STATUS_OPTIONS.map(
    (status) =>
      `<option value="${status}" ${testCase.result === status ? "selected" : ""}>${status}</option>`
  ).join("");
  row.innerHTML = `
    <td><input class="case-name" type="text" value="${escapeAttr(testCase.test_case_name || "")}"></td>
    <td><textarea class="case-objective">${escapeHtml(testCase.objective || "")}</textarea></td>
    <td><select class="case-status">${options}</select></td>
    <td><textarea class="case-comment">${escapeHtml(testCase.comment || "")}</textarea></td>
    <td><input class="case-tasks" type="text" value="${escapeAttr(testCase.tasks || "")}"></td>
    <td><button type="button" class="btn-remove-case">×</button></td>
  `;
  row.querySelector(".btn-remove-case").addEventListener("click", () => row.remove());
  return row;
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function collectCyclesFromDom() {
  return [...document.querySelectorAll("#cycles-root .cycle-block")].map((block, idx) => {
    const cases = [...block.querySelectorAll(".case-body tr")].map((row, caseIdx) => ({
      test_case_key: row.dataset.testCaseKey || `case-${idx + 1}-${caseIdx + 1}`,
      test_case_name: row.querySelector(".case-name").value.trim(),
      result: row.querySelector(".case-status").value,
      objective: row.querySelector(".case-objective").value.trim(),
      comment: row.querySelector(".case-comment").value.trim(),
      tasks: row.querySelector(".case-tasks").value.trim(),
      execution_date: row.dataset.executionDate || "",
      actual_start_date: row.dataset.actualStartDate || "",
      case_iteration_key: row.dataset.caseIterationKey || "",
      logs_source_text: row.dataset.logsSourceText || "",
    }));
    return {
      cycle_id: block.dataset.cycleId || `cycle-${idx + 1}`,
      cycle_key: block.querySelector(".cycle-key").value.trim(),
      cycle_name: block.querySelector(".cycle-name").value.trim(),
      cycle_objective: block.dataset.cycleObjective || "",
      cases,
    };
  });
}

function setSection3Mode(mode) {
  draft.section_3_mode = mode;
  document.querySelectorAll(".mode-btn").forEach((btn) => {
    btn.classList.toggle("primary", btn.dataset.mode === mode);
  });
  document.getElementById("zephyr-panel").classList.toggle("hidden", mode !== "zephyr");
  updateStep3CyclesView();
  if (mode === "zephyr" && currentStep === 3) {
    ensureZephyrTreeLoaded();
  }
}

function updateSelectedFolderLabel() {
  const label = document.getElementById("zephyr-folder-selected");
  if (!label) {
    return;
  }
  label.classList.remove("loading");
  if (zephyrImportLoading && selectedFolder.id) {
    label.textContent = `Импорт кейсов: ${selectedFolder.fullPath || selectedFolder.name}...`;
    label.classList.add("active", "loading");
    return;
  }
  if (selectedCycles.size > 0) {
    label.textContent = `Выбрано циклов: ${selectedCycles.size}`;
    label.classList.add("active");
    return;
  }
  if (!selectedFolder.id) {
    label.textContent = "Папка или циклы не выбраны";
    label.classList.remove("active");
    return;
  }
  if (folderCyclesLoading.has(selectedFolder.id)) {
    label.textContent = `Загрузка циклов: ${selectedFolder.fullPath || selectedFolder.name}...`;
    label.classList.add("active", "loading");
    return;
  }
  const cycleCount = folderCyclesFetched.has(selectedFolder.id)
    ? (folderCyclesByFolder[selectedFolder.id] || []).length
    : 0;
  const cycleHint = cycleCount > 0 ? `, циклов в папке: ${cycleCount}` : "";
  label.textContent = `Выбрана папка: ${selectedFolder.fullPath || selectedFolder.name}${cycleHint}`;
  label.classList.add("active");
}

function applyFolderTreeSelection(node) {
  selectedFolder = {
    id: node.id,
    name: node.name || "",
    fullPath: node.full_path || node.name || node.id,
  };
  selectedCycles.clear();
  expandPathToFolder(node.id);
  folderTreeExpanded.add(node.id);
  updateSelectedFolderLabel();
  renderFolderTree();
  ensureFolderCyclesLoaded(node.id, node.name)
    .then(() => {
      const selectedRow = document.querySelector(`.folder-tree-item[data-node-id="${node.id}"]`);
      selectedRow?.scrollIntoView({ block: "nearest" });
    })
    .catch((err) => showStatus(err.message, true));
}

function toggleCycleTreeSelection(node) {
  const key = cycleNodeKey(node);
  const parent = folderTreeData.find((folder) => folder.id === node.parentFolderId);
  selectedFolder = {
    id: node.parentFolderId,
    name: node.parentFolderName || parent?.name || "",
    fullPath: parent?.full_path || node.parentFolderName || node.parentFolderId,
  };
  if (selectedCycles.has(key)) {
    selectedCycles.delete(key);
  } else {
    selectedCycles.set(key, {
      testRunId: node.testRunId || "",
      cycleKey: node.cycleKey || "",
      label: node.name || node.cycleKey || node.testRunId,
      folderId: node.parentFolderId,
      folderName: node.parentFolderName || parent?.name || "",
    });
  }
  expandPathToFolder(node.parentFolderId);
  folderTreeExpanded.add(node.parentFolderId);
  renderFolderTree();
  updateSelectedFolderLabel();
}

function expandPathToFolder(folderId) {
  const byId = new Map(folderTreeData.map((folder) => [folder.id, folder]));
  let current = byId.get(folderId);
  while (current && current.parent_id) {
    folderTreeExpanded.add(current.parent_id);
    current = byId.get(current.parent_id);
  }
}

function compareFolderNamesNatural(left, right) {
  const leftParts = String(left).match(/(\d+|\D+)/g) || [String(left)];
  const rightParts = String(right).match(/(\d+|\D+)/g) || [String(right)];
  const partCount = Math.max(leftParts.length, rightParts.length);
  for (let index = 0; index < partCount; index += 1) {
    const leftPart = leftParts[index] || "";
    const rightPart = rightParts[index] || "";
    const leftIsNum = /^\d+$/.test(leftPart);
    const rightIsNum = /^\d+$/.test(rightPart);
    if (leftIsNum && rightIsNum) {
      const numericDiff = Number(leftPart) - Number(rightPart);
      if (numericDiff !== 0) {
        return numericDiff;
      }
      continue;
    }
    const textDiff = leftPart.localeCompare(rightPart, "ru", { sensitivity: "base" });
    if (textDiff !== 0) {
      return textDiff;
    }
  }
  return 0;
}

function buildFolderTree(flatList) {
  const byId = new Map();
  flatList.forEach((folder) => {
    byId.set(folder.id, { ...folder, children: [] });
  });
  const roots = [];
  byId.forEach((node) => {
    const parentId = node.parent_id || "";
    if (parentId && byId.has(parentId)) {
      byId.get(parentId).children.push(node);
    } else {
      roots.push(node);
    }
  });
  const sortNodes = (nodes) => {
    nodes.sort((a, b) => compareFolderNamesNatural(a.name || "", b.name || ""));
    nodes.forEach((node) => sortNodes(node.children));
  };
  sortNodes(roots);
  attachCyclesToTree(roots);
  return roots;
}

function makeCycleTreeNode(cycle, parentFolder) {
  const label =
    [cycle.cycle_key, cycle.cycle_name].filter(Boolean).join(" — ") ||
    cycle.test_run_id ||
    cycle.cycle_key;
  return {
    id: `cycle:${cycle.test_run_id || cycle.cycle_key}`,
    name: label,
    full_path: `${parentFolder.full_path || parentFolder.name}/${label}`,
    isCycle: true,
    cycleKey: cycle.cycle_key || "",
    testRunId: cycle.test_run_id || "",
    parentFolderId: parentFolder.id,
    parentFolderName: parentFolder.name || "",
    children: [],
  };
}

function attachCyclesToTree(nodes) {
  nodes.forEach((node) => {
    if (node.isCycle) {
      return;
    }
    const subfolders = node.children.filter((child) => !child.isCycle);
    attachCyclesToTree(subfolders);
    const cycles = (folderCyclesByFolder[node.id] || []).map((cycle) =>
      makeCycleTreeNode(cycle, node)
    );
    cycles.sort((left, right) => compareFolderNamesNatural(left.name, right.name));
    subfolders.sort((left, right) => compareFolderNamesNatural(left.name || "", right.name || ""));
    node.children = [...subfolders, ...cycles];
  });
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function getFolderFilterTokens(filter) {
  return filter
    .trim()
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean);
}

function folderMatchesFilter(node, filter) {
  const tokens = getFolderFilterTokens(filter);
  if (!tokens.length) {
    return true;
  }
  const name = (node.name || "").toLowerCase();
  const path = (node.full_path || "").toLowerCase();
  return tokens.every((token) => name.includes(token) || path.includes(token));
}

function filterTreeNodes(nodes, filter) {
  const filtered = [];
  nodes.forEach((node) => {
    const childMatches = filterTreeNodes(node.children, filter);
    const selfMatch = folderMatchesFilter(node, filter);
    if (selfMatch || childMatches.length) {
      filtered.push({
        ...node,
        children: childMatches.length ? childMatches : node.children,
        forceExpand: true,
      });
      folderTreeFilterExpanded.add(node.id);
    }
  });
  return filtered;
}

function countTreeNodes(nodes) {
  let count = 0;
  nodes.forEach((node) => {
    count += 1;
    count += countTreeNodes(node.children || []);
  });
  return count;
}

function updateFolderFilterHint(filter, visibleCount) {
  const hint = document.getElementById("zephyr-folder-filter-hint");
  updateZephyrLoadingIndicator();
  if (!hint) return;
  if (folderCyclesLoading.size > 0) {
    hint.textContent = "Загрузка тест-циклов для раскрытой папки...";
    hint.classList.remove("hidden");
    return;
  }
  if (!filter) {
    if (folderCyclesFetched.size > 0) {
      const cycleCount = Object.values(folderCyclesByFolder).reduce(
        (sum, items) => sum + items.length,
        0
      );
      hint.textContent = `Загружено циклов: ${cycleCount} (в ${folderCyclesFetched.size} папках)`;
      hint.classList.remove("hidden");
      return;
    }
    hint.textContent = "Раскройте папку (▸), чтобы увидеть тест-циклы";
    hint.classList.remove("hidden");
    return;
  }
  hint.textContent =
    visibleCount > 0
      ? `Найдено папок: ${visibleCount}`
      : "По названию ничего не найдено";
  hint.classList.remove("hidden");
}

function renderFolderTree() {
  const container = document.getElementById("zephyr-folder-tree");
  if (!container) return;
  container.innerHTML = "";
  if (!folderTreeData.length) {
    container.innerHTML = '<p class="folder-tree-hint">Папки не загружены.</p>';
    updateFolderFilterHint("", 0);
    return;
  }
  const filter = (document.getElementById("zephyr-folder-filter")?.value || "")
    .trim()
    .toLowerCase();
  folderTreeFilterExpanded.clear();
  let roots = buildFolderTree(folderTreeData);
  if (filter) {
    roots = filterTreeNodes(roots, filter);
  }
  updateFolderFilterHint(filter, countTreeNodes(roots));
  if (!roots.length) {
    container.innerHTML = '<p class="folder-tree-hint">По запросу ничего не найдено.</p>';
    return;
  }
  const ul = document.createElement("ul");
  ul.className = "folder-tree";
  roots.forEach((node) => ul.appendChild(buildFolderTreeNode(node, 0, filter)));
  container.appendChild(ul);
}

function buildHighlightedName(name, filter) {
  const safeName = escapeHtml(name || "");
  const tokens = getFolderFilterTokens(filter);
  if (!tokens.length) {
    return safeName;
  }
  let html = safeName;
  tokens.forEach((token) => {
    const re = new RegExp(`(${escapeRegex(token)})`, "gi");
    html = html.replace(re, '<mark class="folder-filter-mark">$1</mark>');
  });
  return html;
}

function buildFolderTreeNode(node, depth, filter = "") {
  const li = document.createElement("li");
  const subfolders = (node.children || []).filter((child) => !child.isCycle);
  const hasChildren = subfolders.length > 0 || (node.children || []).length > 0;
  const expanded =
    folderTreeExpanded.has(node.id) ||
    folderTreeFilterExpanded.has(node.id) ||
    Boolean(node.forceExpand);
  const row = document.createElement("div");
  row.className = `folder-tree-item${node.isCycle ? " folder-tree-cycle" : ""}`;
  row.dataset.nodeId = node.id;
  row.style.paddingLeft = `${8 + depth * 16}px`;
  const isSelectedCycle = node.isCycle && selectedCycles.has(cycleNodeKey(node));
  const isSelectedFolder =
    !node.isCycle && selectedFolder.id === node.id && selectedCycles.size === 0;
  if (isSelectedCycle || isSelectedFolder) {
    row.classList.add("selected");
  }
  const cyclesFetched = folderCyclesFetched.has(node.id);
  const isLoadingCycles = folderCyclesLoading.has(node.id);
  const cycleCount = cyclesFetched ? (folderCyclesByFolder[node.id] || []).length : 0;
  if (isLoadingCycles) {
    row.classList.add("loading");
  }

  const toggle = document.createElement("button");
  toggle.type = "button";
  toggle.className = "folder-tree-toggle";
  toggle.setAttribute("aria-label", hasChildren ? "Развернуть или свернуть" : "");
  const hasExpandableContent =
    !node.isCycle && (subfolders.length > 0 || !cyclesFetched || cycleCount > 0);
  if (hasExpandableContent) {
    toggle.textContent = expanded ? "▾" : "▸";
    toggle.addEventListener("click", (event) => {
      event.stopPropagation();
      const willExpand = !folderTreeExpanded.has(node.id);
      if (willExpand) {
        folderTreeExpanded.add(node.id);
        ensureFolderCyclesLoaded(node.id, node.name).catch((err) => showStatus(err.message, true));
      } else {
        folderTreeExpanded.delete(node.id);
      }
      renderFolderTree();
    });
  } else {
    toggle.classList.add("leaf");
    toggle.textContent = "•";
    toggle.disabled = true;
  }

  if (node.isCycle) {
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.className = "folder-tree-checkbox";
    checkbox.checked = selectedCycles.has(cycleNodeKey(node));
    checkbox.addEventListener("click", (event) => {
      event.stopPropagation();
    });
    checkbox.addEventListener("change", () => {
      toggleCycleTreeSelection(node);
    });
    row.appendChild(checkbox);
  } else {
    row.appendChild(toggle);
  }

  const label = document.createElement("span");
  label.className = "folder-tree-label";
  let childCount = "";
  if (!node.isCycle) {
    if (subfolders.length > 0) {
      childCount = ` (${subfolders.length})`;
    } else if (isLoadingCycles) {
      childCount = " (…)";
    } else if (cycleCount > 0) {
      childCount = ` (${cycleCount})`;
    }
  }
  label.innerHTML = `${buildHighlightedName(node.name || node.id, filter)}${escapeHtml(childCount)}`;
  label.title = node.full_path || node.name || node.id;
  row.appendChild(label);
  if (isLoadingCycles) {
    const spinner = document.createElement("span");
    spinner.className = "folder-tree-row-spinner";
    spinner.setAttribute("aria-hidden", "true");
    row.appendChild(spinner);
  }
  row.addEventListener("click", () => {
    if (node.isCycle) {
      toggleCycleTreeSelection(node);
      return;
    }
    applyFolderTreeSelection(node);
  });
  li.appendChild(row);

  if (hasChildren && expanded) {
    const childUl = document.createElement("ul");
    childUl.className = "folder-tree-children";
    node.children.forEach((child) =>
      childUl.appendChild(buildFolderTreeNode(child, depth + 1, filter))
    );
    li.appendChild(childUl);
  }
  return li;
}

function restoreFolderSelectionFromDraft() {
  const source = draft?.zephyr_source || {};
  const folderId = source.folder_id || "";
  if (!folderId) return;
  selectedCycles.clear();
  const savedCycles = Array.isArray(source.selected_cycles) ? source.selected_cycles : [];
  if (savedCycles.length) {
    const folder = folderTreeData.find((item) => item.id === folderId);
    ensureFolderCyclesLoaded(
      folderId,
      folder?.name || source.folder_name || folderId
    )
      .then(() => {
        restoreFolderCycleSelection(folderId, source, savedCycles, folder);
      })
      .catch((err) => showStatus(err.message, true));
    return;
  }
  const folder = folderTreeData.find((item) => item.id === folderId);
  if (folder) {
    applyFolderTreeSelection(folder);
    return;
  }
  applyFolderTreeSelection({
    id: folderId,
    name: source.folder_name || folderId,
    full_path: source.folder_name || folderId,
    children: [],
  });
}

function restoreFolderCycleSelection(folderId, source, savedCycles, folder) {
  savedCycles.forEach((item) => {
      const key = item.test_run_id || item.cycle_key;
      if (!key) return;
      selectedCycles.set(key, {
        testRunId: item.test_run_id || "",
        cycleKey: item.cycle_key || "",
        label: item.cycle_key || item.test_run_id || "",
        folderId: item.folder_id || folderId,
        folderName: item.folder_name || source.folder_name || "",
      });
    });
    selectedFolder = {
      id: folderId,
      name: folder?.name || source.folder_name || folderId,
      fullPath: folder?.full_path || source.folder_name || folderId,
    };
    expandPathToFolder(folderId);
    folderTreeExpanded.add(folderId);
    renderFolderTree();
    updateSelectedFolderLabel();
}

async function ensureFolderCyclesLoaded(folderId, folderName) {
  if (folderCyclesFetched.has(folderId)) {
    return folderCyclesByFolder[folderId] || [];
  }
  if (folderCyclesLoadPromises.has(folderId)) {
    return folderCyclesLoadPromises.get(folderId);
  }
  const promise = (async () => {
    folderCyclesLoading.add(folderId);
    updateZephyrLoadingIndicator();
    updateSelectedFolderLabel();
    renderFolderTree();
    try {
      const params = new URLSearchParams({ folder_name: folderName || "" });
      const result = await api(
        `/api/zephyr/folders/${encodeURIComponent(folderId)}/cycles?${params}`
      );
      folderCyclesByFolder[folderId] = result.cycles || [];
      folderCyclesFetched.add(folderId);
      return folderCyclesByFolder[folderId];
    } finally {
      folderCyclesLoading.delete(folderId);
      folderCyclesLoadPromises.delete(folderId);
      updateZephyrLoadingIndicator();
      renderFolderTree();
      updateSelectedFolderLabel();
    }
  })();
  folderCyclesLoadPromises.set(folderId, promise);
  return promise;
}

function renderStepPills() {
  const root = document.getElementById("step-pills");
  root.innerHTML = "";
  for (let step = 1; step <= MAX_STEP; step += 1) {
    const pill = document.createElement("button");
    pill.type = "button";
    pill.className = `step-pill${step === currentStep ? " active" : ""}`;
    pill.textContent = `Шаг ${step}`;
    pill.addEventListener("click", () => goToStep(step));
    root.appendChild(pill);
  }
  document.querySelectorAll(".step-panel").forEach((panel) => {
    panel.classList.toggle("hidden", Number(panel.dataset.step) !== currentStep);
  });
}

function goToStep(step) {
  syncDraftFromForm();
  currentStep = Math.max(1, Math.min(MAX_STEP, step));
  renderStepPills();
  if (currentStep === 3) {
    ensureZephyrTreeLoaded();
    updateZephyrLoadingIndicator();
  }
  if (currentStep === 4) {
    refreshPreview().catch((err) => showStatus(err.message, true));
  }
}

function showFolderTreeLoading() {
  const container = document.getElementById("zephyr-folder-tree");
  if (!container) return;
  container.innerHTML = '<p class="folder-tree-hint">Загрузка дерева папок...</p>';
}

function ensureZephyrTreeLoaded() {
  if (folderTreeLoading) {
    return;
  }
  if (!folderTreeData.length) {
    loadZephyrFolders({ showSuccess: false }).catch((err) => showStatus(err.message, true));
  }
}

async function loadZephyrFolders({ force = false, showSuccess = true } = {}) {
  if (folderTreeLoading) {
    return;
  }
  if (folderTreeData.length && !force) {
    return;
  }
  if (force) {
    folderCyclesByFolder = {};
    folderCyclesFetched.clear();
    folderCyclesLoading.clear();
    folderCyclesLoadPromises.clear();
    selectedCycles.clear();
  }
  folderTreeLoading = true;
  updateZephyrLoadingIndicator();
  showFolderTreeLoading();
  try {
    const result = await api("/api/zephyr/folders");
    if (result.folder_api_version !== "full-tree-v2") {
      throw new Error(
        "Запущена старая версия сервера (без полного дерева папок). " +
          "Закройте все окна run_universal_report.cmd и запустите снова. " +
          "В консоли должно быть: Folder API: full-tree-v2"
      );
    }
    folderTreeData = result.folders || [];
    folderTreeExpanded.clear();
    folderTreeData.forEach((folder) => {
      if (!folder.parent_id) {
        folderTreeExpanded.add(folder.id);
      }
    });
    renderFolderTree();
    restoreFolderSelectionFromDraft();
    const rootCount = result.root_count ?? folderTreeData.filter((folder) => !folder.parent_id).length;
    if (showSuccess) {
      let message = `Загружено папок: ${folderTreeData.length} (корневых: ${rootCount})`;
      if (result.source) {
        message += `\nИсточник: ${result.source}`;
      }
      if (folderTreeData.length < 100) {
        message +=
          "\nЗагружен не весь проект. Перезапустите run_universal_report.cmd (он остановит старый сервер на порту 8765).";
      }
      showStatus(message, folderTreeData.length < 100);
    }
  } catch (err) {
    const container = document.getElementById("zephyr-folder-tree");
    if (container) {
      container.innerHTML = '<p class="folder-tree-hint">Не удалось загрузить дерево папок.</p>';
    }
    throw err;
  } finally {
    folderTreeLoading = false;
    updateZephyrLoadingIndicator();
  }
}

function showEditor() {
  homeView.classList.add("hidden");
  editorView.classList.remove("hidden");
  currentStep = 1;
  renderStepPills();
  fillFormFromDraft();
}

function showHome() {
  editorView.classList.add("hidden");
  homeView.classList.remove("hidden");
  clearStatus();
}

async function refreshDrafts() {
  const drafts = await api("/api/drafts");
  draftsBody.innerHTML = "";
  if (!drafts.length) {
    draftsBody.innerHTML = `<tr><td colspan="5">Черновиков пока нет</td></tr>`;
    return;
  }
  drafts.forEach((item) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(item.title || "")}</td>
      <td>${escapeHtml(item.report_date || "")}</td>
      <td>${escapeHtml(item.build_name || "")}</td>
      <td>${escapeHtml(item.updated_at || "")}</td>
      <td>
        <button type="button" data-open="${escapeAttr(item.id)}">Открыть</button>
        <button type="button" class="danger" data-delete="${escapeAttr(item.id)}">Удалить</button>
      </td>
    `;
    tr.querySelector("[data-open]").addEventListener("click", async () => {
      draft = await api(`/api/drafts/${item.id}`);
      showEditor();
    });
    tr.querySelector("[data-delete]").addEventListener("click", async () => {
      if (!confirm("Удалить черновик?")) return;
      await api(`/api/drafts/${item.id}`, { method: "DELETE" });
      await refreshDrafts();
    });
    draftsBody.appendChild(tr);
  });
}

async function refreshPreview() {
  syncDraftFromForm();
  const result = await api("/api/preview", {
    method: "POST",
    body: JSON.stringify(draft),
  });
  const frame = document.getElementById("preview-frame");
  frame.srcdoc = result.html;
}

document.getElementById("btn-new-draft").addEventListener("click", async () => {
  draft = await api("/api/drafts/new", { method: "POST", body: "{}" });
  showEditor();
});

document.getElementById("btn-refresh-drafts").addEventListener("click", () => {
  refreshDrafts().catch((err) => showStatus(err.message, true));
});

document.getElementById("btn-back-home").addEventListener("click", () => {
  syncDraftFromForm();
  showHome();
  refreshDrafts().catch(() => {});
});

document.getElementById("btn-prev-step").addEventListener("click", () => goToStep(currentStep - 1));
document.getElementById("btn-next-step").addEventListener("click", () => goToStep(currentStep + 1));

document.getElementById("btn-add-doc-link").addEventListener("click", () => {
  syncDraftFromForm();
  draft.sections_1_2.document_links.push({ label: "", url: "", note: "" });
  renderDocLinks(draft.sections_1_2.document_links);
});

document.getElementById("btn-add-infra").addEventListener("click", () => {
  syncDraftFromForm();
  draft.sections_1_2.infrastructure.push("");
  renderChipList("infra-list", draft.sections_1_2.infrastructure);
});

document.getElementById("btn-add-equip").addEventListener("click", () => {
  syncDraftFromForm();
  draft.sections_1_2.equipment.push("");
  renderChipList("equip-list", draft.sections_1_2.equipment);
});

document.querySelectorAll(".mode-btn").forEach((btn) => {
  btn.addEventListener("click", () => setSection3Mode(btn.dataset.mode));
});

function newCycleId() {
  if (typeof crypto !== "undefined" && crypto.randomUUID) {
    return `cycle-${crypto.randomUUID()}`;
  }
  return `cycle-${Date.now()}-${draft.cycles.length + 1}`;
}

document.getElementById("btn-add-cycle").addEventListener("click", () => {
  syncDraftFromForm();
  draft.cycles.push({
    cycle_id: newCycleId(),
    cycle_key: "",
    cycle_name: "",
    cycle_objective: "",
    cases: [],
  });
  renderCycles(draft.cycles);
});

document.getElementById("zephyr-folder-filter").addEventListener("input", () => {
  renderFolderTree();
});

document.getElementById("btn-clear-folder-filter").addEventListener("click", () => {
  const input = document.getElementById("zephyr-folder-filter");
  if (!input) return;
  input.value = "";
  folderTreeFilterExpanded.clear();
  renderFolderTree();
  input.focus();
});

document.getElementById("btn-import-zephyr").addEventListener("click", async () => {
  clearStatus();
  const payload = {
    folder_id: selectedFolder.id,
    folder_name: selectedFolder.name,
  };
  if (selectedCycles.size > 0) {
    payload.import_mode = "cycles";
    payload.selected_cycles = [...selectedCycles.values()].map((cycle) => ({
      test_run_id: cycle.testRunId,
      cycle_key: cycle.cycleKey,
      folder_id: cycle.folderId,
      folder_name: cycle.folderName,
    }));
    payload.folder_id = selectedFolder.id;
    payload.folder_name = selectedFolder.name;
  } else if (selectedFolder.id) {
    payload.import_mode = "folder";
  } else {
    showStatus("Выберите папку или один/несколько циклов в дереве", true);
    return;
  }
  zephyrImportLoading = true;
  updateZephyrLoadingIndicator();
  updateSelectedFolderLabel();
  try {
    const result = await api("/api/zephyr/import", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    draft.section_3_mode = "zephyr";
    draft.zephyr_source = {
      folder_id: selectedFolder.id,
      folder_name: selectedFolder.name,
      import_mode: payload.import_mode,
      selected_cycles: payload.selected_cycles || [],
      cycle_key: payload.selected_cycles?.[0]?.cycle_key || "",
    };
    draft.cycles = result.cycles;
    updateStep3CyclesView();
    showStatus(`Импортировано циклов: ${result.cycle_count}`);
  } catch (err) {
    showStatus(err.message, true);
  } finally {
    zephyrImportLoading = false;
    updateZephyrLoadingIndicator();
    updateSelectedFolderLabel();
  }
});

document.getElementById("btn-preview").addEventListener("click", () => {
  refreshPreview()
    .then(() => showStatus("Предпросмотр обновлён"))
    .catch((err) => showStatus(err.message, true));
});

document.getElementById("btn-save").addEventListener("click", async () => {
  clearStatus();
  try {
    syncDraftFromForm();
    draft = await api("/api/drafts", { method: "POST", body: JSON.stringify(draft) });
    showStatus("Черновик сохранён");
  } catch (err) {
    showStatus(err.message, true);
  }
});

document.getElementById("btn-build").addEventListener("click", async () => {
  clearStatus();
  try {
    syncDraftFromForm();
    const result = await api("/api/build", { method: "POST", body: JSON.stringify(draft) });
    showStatus(`Отчёт собран:\n${result.paths.join("\n")}`);
  } catch (err) {
    showStatus(err.message, true);
  }
});

document.getElementById("btn-publish").addEventListener("click", async () => {
  clearStatus();
  try {
    syncDraftFromForm();
    const result = await api("/api/publish", { method: "POST", body: JSON.stringify(draft) });
    showStatus(`Опубликовано:\n${(result.outcomes || []).join("\n")}`);
  } catch (err) {
    showStatus(err.message, true);
  }
});

refreshDrafts().catch((err) => showStatus(err.message, true));
