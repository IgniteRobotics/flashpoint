## Why

Anomaly detection says that something is unusual. Prediction says when a unit will fail. That requires labeled failures tied to physical units, which only becomes trustworthy once serial-number identity exists (D11). Roadmap phase **P6** (stretch).

## What Changes

- A **maintenance records** store keyed by serial number: failure, swap, rebuild, and inspection events, entered by students in the pit
- Degradation models per unit: thermal rise-rate drift, residual drift, and odometry-based wear
- A remaining-useful-life estimate with confidence, shown on the unit page
- A spare-rotation recommendation report

## Capabilities

### New Capabilities
- `maintenance-records`: capturing maintenance and failure events per unit, and querying them
- `failure-prediction`: degradation tracking and remaining-life estimates

### Modified Capabilities
- `unit-odometry`: shows the prediction and the maintenance history

## Non-goals

- Pooling data across teams (revisit after a season of data)

## Impact

New `src/flashpoint/predict/`, plus maintenance entry UI. Depends on P5 baselines and P2 unit identity.
