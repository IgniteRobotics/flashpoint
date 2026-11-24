/* Diagram notation (spec project-documentation, "One diagram notation"; docs-diagrams D2).
   main.html loads this right after the vendored Mermaid, before the theme's bundle, on pages with
   a diagram only. The theme calls mermaid.initialize() once with its own settings, then draws
   each diagram into a closed shadow root that no page script can reach. So this wraps that one
   call to add:
   - plain SVG text labels (`htmlLabels: false`): Safari and the iOS app clip HTML labels;
   - the font the theme draws labels in, so each box is sized for its text;
   - wide wrapping, so the author's own line breaks (`<br>`) decide the lines;
   - the kind colours (KIND_CSS), read from custom properties that diagrams.css defines from the
     docs palette. They inherit into the shadow root, so the toggle recolours a drawn diagram.
   Each render also waits for the page's fonts, so boxes are measured in the loaded font. */
(() => {
  'use strict';

  if (typeof mermaid === 'undefined') return;

  const WRAP_PX = 480;
  const SHAPES = (kind) =>
    ['.label-container', '> rect', '> path', '> polygon', '> circle']
      .map((shape) => `.node.${kind} ${shape}`)
      .join(',');
  const KIND_CSS = ['step', 'data', 'robot', 'outside', 'error']
    .map(
      (kind) => `${SHAPES(kind)}{fill:var(--fp-diagram-${kind}-fill);` +
        `stroke:var(--fp-diagram-${kind});stroke-width:1.5px}`,
    )
    .join('') +
    `${SHAPES('outside')}{stroke-dasharray:5 4}` +
    // Plain SVG text takes its colour from `fill`, not `color`.
    '.label text,.nodeLabel,text.nodeLabel,.edgeLabel text,g.stateGroup text,.cluster-label text' +
    '{fill:var(--md-mermaid-label-fg-color);font-family:var(--md-mermaid-font-family)}' +
    '.edgeLabel text{fill:var(--md-mermaid-edge-color)}' +
    // Mermaid's own rules for edge label backgrounds are scoped to the diagram's id, and
    // half-transparent: lines would show through the text.
    '.edgeLabel rect.background{fill:var(--md-mermaid-label-bg-color)!important;opacity:1!important}';

  const font = () =>
    getComputedStyle(document.body).getPropertyValue('--md-mermaid-font-family').trim();

  const initialize = mermaid.initialize.bind(mermaid);
  mermaid.initialize = (config = {}) => {
    const family = font();
    initialize({
      ...config,
      ...(family ? { fontFamily: family } : {}),
      htmlLabels: false,
      themeCSS: `${config.themeCSS || ''}${KIND_CSS}`,
      flowchart: { ...(config.flowchart || {}), htmlLabels: false, wrappingWidth: WRAP_PX },
      // A state's minimum width pads its label's box, but plain text isn't centred in it.
      state: { ...(config.state || {}), htmlLabels: false, wrappingWidth: WRAP_PX, minNodeWidth: 0 },
    });
  };

  const render = mermaid.render.bind(mermaid);
  mermaid.render = async (...args) => {
    if (document.fonts) await document.fonts.ready;
    return render(...args);
  };

  // Diagram zoom (docs-diagrams D4): before the theme replaces each `pre.mermaid` with its
  // drawing, wrap it in a focusable box that keeps the source. zoom.js opens the box by drawing
  // the source again, since the drawing on the page sits in a closed shadow root.
  const heading = (el) => {
    for (let node = el.previousElementSibling; node; node = node.previousElementSibling) {
      if (/^H[1-6]$/.test(node.tagName)) return node.textContent.replace(/¶$/, '').trim();
    }
    return '';
  };
  for (const pre of document.querySelectorAll('.md-content pre.mermaid')) {
    const box = document.createElement('div');
    box.className = 'fp-diagram';
    box.dataset.fpSource = pre.textContent;
    const name = heading(pre);
    box.dataset.fpLabel = name ? `Diagram: ${name}` : 'Diagram';
    pre.replaceWith(box);
    box.append(pre);
  }
})();
