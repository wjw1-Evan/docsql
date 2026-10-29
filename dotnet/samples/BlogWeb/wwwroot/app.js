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

// ---- 启动 ----

(async () => {
  try {
    await Promise.all([loadStats(), loadPosts(), loadAuthors()]);
    if (state.posts.length > 0) await openPost(state.posts[0].id);
  } catch (err) {
    document.body.append(el("p", "err", `加载失败:${err.message}`));
  }
})();
