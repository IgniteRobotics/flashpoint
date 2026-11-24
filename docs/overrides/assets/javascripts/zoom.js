/* Click to zoom (spec project-documentation, "Images open full-frame"; docs-visual-polish D4).
   A screenshot, GIF or animation in the page body opens full-frame in a modal dialog, by click,
   Enter or Space. A click anywhere in it, its close button or Esc closes it, and focus goes back
   to the image. No third-party code, so it works in the offline archive. Images inside a link,
   and the logo and icons, are left alone.
   A diagram (`.fp-diagram`, made by diagrams.js; spec "Diagrams open full-frame") opens the same
   way. The theme draws it in a closed shadow root, so it's drawn again from its source with the
   site's own Mermaid, at the largest size that shows all of it. */
(() => {
  'use strict';

  const MEDIA = '.md-content img, .md-content video, .md-content .fp-diagram';
  const MARGIN_PX = 16;
  const LEAVE = 'a, .md-logo, .md-icon, .twemoji, .emojione, .gemoji';
  let dialog = null;
  let figure = null;
  let caption = null;
  let source = null;
  let drawn = 0;

  const zoomable = (el) => el instanceof Element && el.matches(MEDIA) && !el.closest(LEAVE);
  const isDiagram = (el) => el.classList.contains('fp-diagram');
  const label = (el) =>
    el.getAttribute('alt') || el.dataset.fpLabel || el.getAttribute('aria-label') || '';

  function build() {
    dialog = document.createElement('dialog');
    dialog.className = 'fp-zoom';
    const close = document.createElement('button');
    close.type = 'button';
    close.className = 'fp-zoom__close';
    close.setAttribute('aria-label', 'Close');
    close.textContent = '×';
    figure = document.createElement('figure');
    figure.className = 'fp-zoom__figure';
    caption = document.createElement('figcaption');
    caption.className = 'fp-zoom__caption';
    dialog.append(close, figure);
    // Any click inside closes: the image, the backdrop around it, or the close button. The copy
    // goes at once: the `close` event comes a moment later, and the page could find two images.
    dialog.addEventListener('click', () => {
      figure.replaceChildren();
      dialog.close();
    });
    window.addEventListener('resize', () => {
      if (dialog.open) fit();
    });
    dialog.addEventListener('close', () => {
      figure.replaceChildren();
      if (source) source.focus({ preventScroll: true });
      source = null;
    });
    document.body.append(dialog);
  }

  // The opened diagram at the largest size that shows all of it, above the caption.
  function fit() {
    const card = figure.querySelector('.fp-zoom__diagram');
    const svg = card && card.querySelector('svg');
    if (!svg) return;
    const box = svg.viewBox.baseVal;
    const pad = parseFloat(getComputedStyle(card).paddingLeft) || 0;
    const width = window.innerWidth - 2 * MARGIN_PX - 2 * pad;
    const height = window.innerHeight - caption.offsetHeight - 2 * MARGIN_PX - 2 * pad;
    const scale = Math.min(width / box.width, height / box.height);
    svg.setAttribute('width', String(box.width * scale));
    svg.setAttribute('height', String(box.height * scale));
  }

  async function openDiagram(el) {
    const { svg, bindFunctions } = await mermaid.render(`fp-zoom-${drawn += 1}`,
      el.dataset.fpSource);
    const card = document.createElement('div');
    card.className = 'fp-zoom__diagram';
    card.innerHTML = svg;
    const drawing = card.querySelector('svg');
    drawing.removeAttribute('style');  // Mermaid caps it at its natural width
    drawing.setAttribute('role', 'img');
    drawing.setAttribute('aria-label', label(el));
    caption.textContent = label(el);
    figure.replaceChildren(card, caption);
    if (bindFunctions) bindFunctions(card);
    dialog.setAttribute('aria-label', label(el));
    source = el;
    dialog.showModal();
    fit();
  }

  function open(el) {
    if (!dialog) build();
    if (isDiagram(el)) {
      openDiagram(el).catch((error) => console.warn('Flashpoint: diagram zoom failed', error));
      return;
    }
    const copy = el.cloneNode(true);
    for (const name of ['id', 'tabindex', 'role', 'aria-label', 'width', 'height', 'style']) {
      copy.removeAttribute(name);
    }
    copy.classList.remove('fp-zoomable');
    copy.setAttribute('alt', label(el));
    if (copy instanceof HTMLVideoElement) {
      copy.muted = true;
      copy.loop = true;
      copy.autoplay = true;
      copy.playsInline = true;
      copy.currentTime = el.currentTime;
    }
    caption.textContent = label(el);
    figure.replaceChildren(copy, caption);
    dialog.setAttribute('aria-label', label(el) || 'Enlarged image');
    source = el;
    dialog.showModal();
    if (copy instanceof HTMLVideoElement) copy.play().catch(() => {});
  }

  for (const el of document.querySelectorAll(MEDIA)) {
    if (!zoomable(el)) continue;
    el.classList.add('fp-zoomable');
    el.tabIndex = 0;
    el.setAttribute('role', 'button');
    if (el instanceof HTMLImageElement || isDiagram(el)) {
      el.setAttribute('aria-label', `Open full size: ${label(el)}`);
    }
  }

  document.addEventListener('click', (event) => {
    const el = event.target instanceof Element ? event.target.closest('.fp-zoomable') : null;
    if (!el || !zoomable(el)) return;
    event.preventDefault();
    open(el);
  });
  document.addEventListener('keydown', (event) => {
    const el = event.target;
    if (!(el instanceof Element) || !el.classList.contains('fp-zoomable') || !zoomable(el)) return;
    if (event.key !== 'Enter' && event.key !== ' ') return;
    event.preventDefault();
    open(el);
  });
})();
