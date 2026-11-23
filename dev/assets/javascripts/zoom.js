/* Click to zoom (spec project-documentation, "Images open full-frame"; docs-visual-polish D4).
   A screenshot, GIF or animation in the page body opens full-frame in a modal dialog, by click,
   Enter or Space. A click anywhere in it, its close button or Esc closes it, and focus goes back
   to the image. No third-party code, so it works in the offline archive. Images inside a link,
   and the logo and icons, are left alone. */
(() => {
  'use strict';

  const MEDIA = '.md-content img, .md-content video';
  const LEAVE = 'a, .md-logo, .md-icon, .twemoji, .emojione, .gemoji';
  let dialog = null;
  let figure = null;
  let caption = null;
  let source = null;

  const zoomable = (el) => el instanceof Element && el.matches(MEDIA) && !el.closest(LEAVE);
  const label = (el) => el.getAttribute('alt') || el.getAttribute('aria-label') || '';

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
    // Any click inside closes: the image, the backdrop around it, or the close button.
    dialog.addEventListener('click', () => dialog.close());
    dialog.addEventListener('close', () => {
      figure.replaceChildren();
      if (source) source.focus({ preventScroll: true });
      source = null;
    });
    document.body.append(dialog);
  }

  function open(el) {
    if (!dialog) build();
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
    if (el instanceof HTMLImageElement) el.setAttribute('aria-label', `Open full size: ${label(el)}`);
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
