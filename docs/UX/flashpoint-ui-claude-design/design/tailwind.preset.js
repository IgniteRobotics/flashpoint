/**
 * Mnemosyne Tailwind preset. Mirrors design/tokens.json.
 * Colors resolve to CSS custom properties from tokens.css so a single source drives both.
 *
 * Usage (tailwind.config.js):
 *   module.exports = { presets: [require('./design/mnemosyne-ui-handoff/design/tailwind.preset.js')], content: [...] }
 *
 * Tailwind v4 note: if you're on v4's CSS-first config, translate this to an @theme block
 * that maps the same names to the same var(--…) values.
 */
const v = (name) => `var(--${name})`;

module.exports = {
  theme: {
    // Replace, don't extend: no stray default palette or radii.
    colors: {
      transparent: 'transparent',
      current: 'currentColor',
      bg: v('bg'),
      surface: {
        1: v('surface-1'), 2: v('surface-2'), 3: v('surface-3'),
        raised: v('surface-raised'), drag: v('surface-drag'),
      },
      line: { soft: v('line-soft'), DEFAULT: v('line'), strong: v('line-strong') },
      amber: {
        DEFAULT: v('amber'), hi: v('amber-hi'), mid: v('amber-mid'),
        dim: v('amber-dim'), muted: v('amber-muted'),
      },
      prose: v('prose'),
      hot: v('hot'),
      'on-amber': v('on-amber'),
    },
    borderRadius: { none: '0', DEFAULT: '0' },
    fontFamily: {
      display: ['VT323', 'ui-monospace', 'Cascadia Mono', 'Menlo', 'monospace'],
      ui: ['IBM Plex Mono', 'ui-monospace', 'Cascadia Mono', 'Menlo', 'Consolas', 'monospace'],
      prose: ['IBM Plex Sans', 'Segoe UI', 'system-ui', '-apple-system', 'sans-serif'],
    },
    fontSize: {
      micro: ['11px', { lineHeight: '1.4', letterSpacing: '0.2em' }],
      xs: ['12px', { lineHeight: '1.5' }],
      sm: ['13px', { lineHeight: '1.5' }],
      base: ['14px', { lineHeight: '1.5' }],
      md: ['15px', { lineHeight: '1.6' }],
      prose: ['17px', { lineHeight: '1.7' }],
      lg: ['20px', { lineHeight: '1.4' }],
      d1: ['38px', { lineHeight: '1', letterSpacing: '0.18em' }],
      d2: ['44px', { lineHeight: '1' }],
      d3: ['52px', { lineHeight: '1' }],
      d4: ['72px', { lineHeight: '0.95' }],
    },
    letterSpacing: { normal: '0', label: '0.2em', button: '0.14em', brand: '0.18em', meta: '0.12em' },
    spacing: {
      0: '0', px: '1px', 1: '4px', 2: '8px', 3: '12px', 4: '16px', 5: '20px', 6: '24px',
      7: '28px', 8: '32px', 10: '40px', 16: '64px', hit: '44px', 'hit-sm': '36px',
    },
    extend: {
      minHeight: { hit: '44px', 'hit-sm': '36px' },
      maxWidth: { measure: '760px' },
      boxShadow: {
        'glow-text': '0 0 10px rgba(255,176,0,0.40)',
        'glow-select': '0 0 18px rgba(255,176,0,0.45)',
        'glow-panel': '0 0 22px rgba(255,176,0,0.15)',
        'glow-line': '0 0 6px rgba(255,176,0,0.60)',
      },
      transitionDuration: { fast: '150ms', base: '200ms', layout: '420ms' },
      transitionTimingFunction: { mnemo: 'cubic-bezier(.2,.7,.2,1)' },
      keyframes: { blink: { '50%': { opacity: '0' } } },
      animation: { blink: 'blink 1s steps(1) infinite' },
    },
  },
};
