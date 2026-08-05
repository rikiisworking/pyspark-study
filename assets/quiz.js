/**
 * Tiny quiz + free-text drill helpers for lessons.
 * Usage:
 *   Quiz.mount(el, { prompt, options: [{text, correct, why}], shuffle?: true })
 *   Drill.mount(el, { prompt, normalize, accept, solution })
 */
(function (global) {
  function shuffle(arr) {
    const a = arr.slice();
    for (let i = a.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [a[i], a[j]] = [a[j], a[i]];
    }
    return a;
  }

  const Quiz = {
    mount(root, cfg) {
      if (typeof root === "string") root = document.querySelector(root);
      if (!root) return;
      root.classList.add("quiz");
      const opts = cfg.shuffle === false ? cfg.options.slice() : shuffle(cfg.options);
      root.innerHTML = "";
      const q = document.createElement("div");
      q.className = "q";
      q.textContent = cfg.prompt;
      root.appendChild(q);
      const box = document.createElement("div");
      box.className = "opts";
      const fb = document.createElement("div");
      fb.className = "fb";
      let locked = false;
      opts.forEach((o) => {
        const b = document.createElement("button");
        b.type = "button";
        b.className = "opt";
        b.textContent = o.text;
        b.addEventListener("click", () => {
          if (locked) return;
          locked = true;
          box.querySelectorAll("button").forEach((x) => (x.disabled = true));
          if (o.correct) {
            b.classList.add("correct");
            fb.className = "fb ok";
            fb.textContent = o.why || "Correct.";
          } else {
            b.classList.add("wrong");
            const right = opts.find((x) => x.correct);
            if (right) right._btn && right._btn.classList.add("correct");
            fb.className = "fb bad";
            fb.textContent = o.why || (right ? "Answer: " + right.text : "Wrong.");
          }
        });
        o._btn = b;
        box.appendChild(b);
      });
      root.appendChild(box);
      root.appendChild(fb);
    },
  };

  function defaultNormalize(s) {
    return String(s)
      .replace(/#.*$/gm, "")
      .replace(/["']/g, '"')
      .replace(/\s+/g, " ")
      .replace(/\s*([().,=<>!|&+\-*/])\s*/g, "$1")
      .trim()
      .toLowerCase();
  }

  const Drill = {
    mount(root, cfg) {
      if (typeof root === "string") root = document.querySelector(root);
      if (!root) return;
      root.classList.add("drill");
      const norm = cfg.normalize || defaultNormalize;
      const accept = Array.isArray(cfg.accept) ? cfg.accept : [cfg.accept];
      root.innerHTML = "";
      const lab = document.createElement("label");
      lab.textContent = cfg.prompt;
      const ta = document.createElement("textarea");
      ta.spellcheck = false;
      ta.placeholder = "type from memory…";
      const actions = document.createElement("div");
      actions.className = "actions";
      const check = document.createElement("button");
      check.type = "button";
      check.textContent = "Check";
      const show = document.createElement("button");
      show.type = "button";
      show.className = "secondary";
      show.textContent = "Show solution";
      const fb = document.createElement("div");
      fb.className = "fb";
      const rev = document.createElement("div");
      rev.className = "reveal";
      const pre = document.createElement("pre");
      const code = document.createElement("code");
      code.textContent = cfg.solution;
      pre.appendChild(code);
      rev.appendChild(pre);
      check.addEventListener("click", () => {
        const got = norm(ta.value);
        const ok = accept.some((a) => norm(a) === got);
        fb.className = "fb " + (ok ? "ok" : "bad");
        fb.textContent = ok
          ? "Match. Lock it in — rewrite once more without peeking."
          : "No match. Whitespace/quotes flexible; try again or show solution.";
      });
      show.addEventListener("click", () => {
        rev.classList.add("show");
        fb.className = "fb";
        fb.textContent = "Cover this, wait 10s, retype from blank.";
      });
      actions.appendChild(check);
      actions.appendChild(show);
      root.appendChild(lab);
      root.appendChild(ta);
      root.appendChild(actions);
      root.appendChild(fb);
      root.appendChild(rev);
    },
  };

  global.LessonUI = { Quiz, Drill, shuffle };
})(typeof window !== "undefined" ? window : globalThis);
