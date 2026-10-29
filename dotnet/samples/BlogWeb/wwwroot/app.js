// DocSQL 博客前端(零依赖):列表 / 标签 / 搜索 / 阅读 / 评论 / 发文章。
// 所有文本都用 textContent 写入 DOM(文章与评论内容按数据渲染,不拼 HTML)。

const state = {
  posts: [],
  tags: [],
  tag: "",
  q: "",
  selected: null,
};

const $ = (sel) => document.querySelector(sel);

const api = {
  async get(path) {
    const r = await fetch(path);
    if (!r.ok) throw new Error((await r.json().catch(() => ({}))).error || r.statusText);
    return r.json();
  },
  async post(path, body) {
    const r = await fetch(path, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.error || r.statusText);
    return data;
  },
};

function fmtDate(iso) {
  const d = new Date(iso);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(
    d.getDate()
  ).padStart(2, "0")} ${String(d.getHours()).padStart(2, "0")}:${String(
    d.getMinutes()
  ).padStart(2, "0")}`;
}

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function avatar(name) {
  return el("span", "avatar", (name || "?").slice(0, 1).toUpperCase());
}

// ---- 顶部统计 ----

async function loadStats() {
  const s = await api.get("/api/stats");
  const box = $("#stats");
  box.replaceChildren(
    statItem("文章", s.posts),
    statItem("评论", s.comments),
    statItem("作者", s.authors)
  );
  $("#mode-badge").textContent = s.embedded ? "嵌入式 · 零守护" : "远程节点";
  if (s.embedded) {
    $("#mode-badge").title = `数据文件:${s.database}`;
  }
}

function statItem(label, n) {
  const span = el("span");
  span.append(el("b", null, String(n)), document.createTextNode(` ${label}`));
  return span;
}

// ---- 文章列表 ----

async function loadPosts() {
  const params = new URLSearchParams();
  if (state.tag) params.set("tag", state.tag);
  if (state.q) params.set("q", state.q);
  state.posts = await api.get(`/api/posts?${params}`);
  const tags = new Set(state.tags);
  for (const p of state.posts) for (const t of p.tags || []) tags.add(t);
  state.tags = [...tags].sort();
  renderTags();
  renderPosts();
}

function renderTags() {
  const bar = $("#tags");
  bar.replaceChildren();
  const all = el("button", "chip", "全部");
  if (!state.tag) all.classList.add("active");
  all.onclick = () => {
    state.tag = "";
    void loadPosts();
  };
  bar.append(all);
  for (const t of state.tags) {
    const chip = el("button", "chip", `#${t}`);
    if (state.tag === t) chip.classList.add("active");
    chip.onclick = () => {
      state.tag = state.tag === t ? "" : t;
      void loadPosts();
    };
    bar.append(chip);
  }
}

function renderPosts() {
  const list = $("#posts");
  list.replaceChildren();
  $("#empty").hidden = state.posts.length !== 0;
  for (const p of state.posts) {
    const card = el("article", "card");
    if (state.selected === p.id) card.classList.add("active");
    card.onclick = () => void openPost(p.id);

    card.append(el("h3", null, p.title));
    const meta = el("div", "meta");
    meta.append(avatar(p.author), el("span", null, p.author), el("span", null, fmtDate(p.createdAt)));
    for (const t of p.tags || []) meta.append(el("span", "tag", `#${t}`));
    meta.append(el("span", "count", `💬 ${p.comments}`));
    card.append(meta);

    const excerpt = (p.body || "").replace(/\s+/g, " ").trim();
    if (excerpt) card.append(el("p", "excerpt", excerpt));
    list.append(card);
  }
}

// ---- 详情与评论 ----

async function openPost(id) {
  state.selected = id;
  renderPosts();
  const post = await api.get(`/api/posts/${id}`);
  const pane = $("#detail");
  pane.replaceChildren();
  const wrap = el("div", "detail");

  wrap.append(el("h2", null, post.title));
  const meta = el("div", "meta");
  meta.append(avatar(post.author), el("span", null, post.author), el("span", null, fmtDate(post.createdAt)));
  for (const t of post.tags || []) meta.append(el("span", "tag", `#${t}`));
  wrap.append(meta);

  wrap.append(el("div", "body", post.body || "(无正文)"));

  // 类型演示行:DECIMAL 收益 + JSON 元数据;点赞按钮 = ON CONFLICT upsert。
  const types = el("div", "types");
  types.append(el("span", "type-chip", `DECIMAL 收益:${post.payout ?? "NULL"}`));
  if (post.meta) types.append(el("span", "type-chip", `JSON 元数据:${trunc(post.meta, 48)}`));
  wrap.append(types);

  const actions = el("div", "post-actions");
  const likeBtn = el("button", "like", `👍 点赞 ${post.likes ?? 0}`);
  likeBtn.onclick = async () => {
    likeBtn.disabled = true;
    try {
      const r = await api.post(`/api/posts/${post.id}/like`, {});
      likeBtn.textContent = `👍 点赞 ${r.likes}`;
    } catch (err) {
      alert(err.message);
    } finally {
      likeBtn.disabled = false;
    }
  };
  actions.append(likeBtn, el("span", "views", `👁 ${post.views ?? 0}`));
  wrap.append(actions);

  wrap.append(el("h3", null, `评论 (${post.comments.length})`));
  const list = el("div", "comments");
  if (post.comments.length === 0) list.append(el("p", "empty", "还没有评论,来说两句。"));
  for (const c of post.comments) {
    const item = el("div", "comment");
    item.append(el("div", "meta", fmtDate(c.createdAt)));
    item.append(el("p", null, c.text));
    list.append(item);
  }
  wrap.append(list);

  const form = el("form", "comment-form");
  const input = el("input");
  input.placeholder = "写评论…";
  input.maxLength = 2000;
  input.required = true;
  const btn = el("button", "primary", "发送");
  btn.type = "submit";
  form.append(input, btn);
  form.onsubmit = async (e) => {
    e.preventDefault();
    const text = input.value.trim();
    if (!text) return;
    btn.disabled = true;
    try {
      await api.post(`/api/posts/${post.id}/comments`, { text });
      input.value = "";
      await Promise.all([openPost(post.id), loadPosts(), loadStats()]);
    } catch (err) {
      alert(err.message);
    } finally {
      btn.disabled = false;
    }
  };
  wrap.append(form);

  pane.append(wrap);
}

// ---- 发文章 ----

const composer = $("#composer");
const form = $("#post-form");

$("#compose-btn").onclick = () => {
  form.reset();
  $("#form-err").textContent = "";
  composer.showModal();
};
$("#cancel-compose").onclick = () => composer.close();

form.onsubmit = async (e) => {
  e.preventDefault();
  const data = new FormData(form);
  const tags = String(data.get("tags") || "")
    .split(/[,，\s]+/)
    .map((x) => x.trim())
    .filter(Boolean);
  $("#form-err").textContent = "";
  try {
    const created = await api.post("/api/posts", {
      title: data.get("title"),
      body: data.get("body"),
      author: data.get("author"),
      tags,
    });
    composer.close();
    await Promise.all([loadPosts(), loadStats()]);
    await openPost(created.id);
  } catch (err) {
    $("#form-err").textContent = err.message;
  }
};

// 搜索防抖
let searchTimer;
$("#search").addEventListener("input", (e) => {
  clearTimeout(searchTimer);
  const value = e.target.value.trim();
  searchTimer = setTimeout(() => {
    state.q = value;
    void loadPosts();
  }, 250);
});

async function loadAuthors() {
  const authors = await api.get("/api/authors");
  const list = $("#author-list");
  list.replaceChildren(...authors.map((a) => {
    const opt = document.createElement("option");
    opt.value = a.name;
    return opt;
  }));
}

// ---- 数据库面板(状态 + 写路径演示 + 功能面板) ----

const dbpanel = $("#dbpanel");

$("#db-btn").onclick = () => {
  dbpanel.showModal();
  void loadDbPanel();
};
$("#db-close").onclick = () => dbpanel.close();

function fmtBytes(n) {
  if (n == null) return "—";
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(2)} MB`;
}

function fmtUptime(sec) {
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;
  return h > 0 ? `${h}时${m}分` : m > 0 ? `${m}分${s}秒` : `${s}秒`;
}

function trunc(text, max = 60) {
  const s = String(text);
  return s.length > max ? `${s.slice(0, max)}…` : s;
}

function kvCell(label, value, mono = false) {
  const cell = el("div", "kv");
  cell.append(el("span", "kv-label", label));
  const v = el("span", mono ? "kv-value mono" : "kv-value", value);
  v.title = String(value);
  cell.append(v);
  return cell;
}

function resultTable(item) {
  if (item.error) {
    const box = el("div", "lab-error", `执行报错:${item.error}`);
    return box;
  }
  const table = el("table", "lab-table");
  const head = el("tr");
  for (const c of item.columns) head.append(el("th", null, c));
  table.append(head);
  for (const row of item.rows) {
    const tr = el("tr");
    for (const v of row) {
      const text = v === null || v === undefined
        ? "NULL"
        : v instanceof Object ? JSON.stringify(v) : String(v);
      tr.append(el("td", null, trunc(text)));
    }
    table.append(tr);
  }
  if (item.rows.length === 0) {
    const tr = el("tr");
    tr.append(el("td", "lab-empty", "(0 行)"));
    table.append(tr);
  }
  return table;
}

async function loadDbPanel() {
  $("#lab-action-result").hidden = true;
  const [status, lab] = await Promise.all([
    api.get("/api/status"),
    api.get("/api/lab"),
  ]);

  // 状态区
  const box = $("#db-status");
  box.replaceChildren(
    kvCell("模式", status.embedded ? "嵌入式(进程内引擎)" : "远程节点"),
    kvCell("数据库", status.database, true),
    kvCell("引擎", `${status.engine.name} · DB_NAME=${status.engine.dbName}`),
    kvCell("客户端版本", status.engine.clientVersion || "—"),
    kvCell("本进程运行时长", fmtUptime(status.engine.uptimeSeconds)),
    kvCell("数据文件", status.storage ? fmtBytes(status.storage.dataFileBytes) : "远程(在节点侧)"),
    kvCell("WAL", status.storage ? fmtBytes(status.storage.walBytes) : "远程(在节点侧)"),
    kvCell("订阅数(auto-GUID 表)", String(status.subscribers))
  );
  const obj = el("div", "db-objects");
  const objTitle = el("h4", null, `对象清单(表 ${status.objects.tables.length} · 视图 ${status.objects.views.length} · 索引 ${status.objects.indexes.length})`);
  obj.append(objTitle);
  const tableRows = el("div", "obj-rows");
  for (const t of status.objects.tables) {
    const row = el("div", "obj-row");
    row.append(el("span", "obj-name", t.name + (t.name.startsWith("mv_") ? " (物化视图)" : "")));
    row.append(el("span", "obj-count", `${t.rowCount} 行`));
    tableRows.append(row);
  }
  obj.append(tableRows);
  if (status.objects.views.length > 0) {
    obj.append(el("div", "obj-note", `视图:${status.objects.views.join("、")}`));
  }
  const idx = status.objects.indexes.map((i) => `${i.name}→${i.table}`).join("、");
  if (idx) obj.append(el("div", "obj-note", `索引:${idx}`));
  box.append(obj);

  // 功能面板(按组渲染折叠项)
  const labBox = $("#lab");
  labBox.replaceChildren();
  let lastGroup = "";
  for (const item of lab) {
    if (item.group !== lastGroup) {
      lastGroup = item.group;
      labBox.append(el("h4", "lab-group", item.group));
    }
    const details = el("details", "lab-item");
    const summary = el("summary");
    summary.append(el("span", null, item.feature), el("span", "lab-id", item.id));
    details.append(summary);
    details.append(el("pre", "lab-sql", item.sql));
    details.append(resultTable(item));
    labBox.append(details);
  }
}

// 写路径演示按钮
$("#lab-actions").addEventListener("click", async (e) => {
  const btn = e.target.closest("button[data-action]");
  if (!btn) return;
  const out = $("#lab-action-result");
  out.hidden = false;
  out.textContent = "执行中…";
  btn.disabled = true;
  try {
    const r = await api.post(`/api/lab/action/${btn.dataset.action}`, {});
    out.textContent = JSON.stringify(r, null, 2);
  } catch (err) {
    out.textContent = `失败:${err.message}`;
  } finally {
    btn.disabled = false;
  }
});

// 订阅(auto-GUID 演示)
$("#sub-btn").onclick = async () => {
  const input = $("#sub-email");
  const out = $("#lab-action-result");
  out.hidden = false;
  out.textContent = "执行中…";
  try {
    const r = await api.post("/api/subscribers", { email: input.value.trim() });
    out.textContent = `订阅成功 → 引擎生成的 UUIDv7 主键:${r.id}`;
    input.value = "";
    void loadStats();
  } catch (err) {
    out.textContent = `失败:${err.message}`;
  }
};

// ---- 启动 ----

(async () => {
  try {
    await Promise.all([loadStats(), loadPosts(), loadAuthors()]);
    if (state.posts.length > 0) await openPost(state.posts[0].id);
  } catch (err) {
    document.body.append(el("p", "err", `加载失败:${err.message}`));
  }
})();
