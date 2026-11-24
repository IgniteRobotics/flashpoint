# A page with a tagged diagram

```mermaid
flowchart TB
    classDef outside stroke-dasharray:5 4
    R[the robot]:::robot -->|acquire| I[(inbox)]
    M[logs by hand] -->|ingest| I
    I --> APP[the app]
    class I data
    class M outside
    class APP step
```

<div class="fp-legend">
  <span class="fp-legend__item"><span class="fp-swatch fp-swatch--step"></span>a step</span>
  <span class="fp-legend__item"><span class="fp-swatch fp-swatch--data"></span>data</span>
  <span class="fp-legend__item"><span class="fp-swatch fp-swatch--robot"></span>the robot</span>
  <span class="fp-legend__item"><span class="fp-swatch fp-swatch--outside"></span>outside</span>
</div>
