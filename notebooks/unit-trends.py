import marimo

__generated_with = "0.16.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl

    from flashpoint import config
    from flashpoint.lake.paths import LakePaths
    from flashpoint.report.settings import load_report_config
    from flashpoint.semantics.robot_config import load_robots
    from flashpoint.views.queries import METRICS, Filters, HistoryQueries

    return (
        Filters, HistoryQueries, LakePaths, METRICS, config, load_report_config, load_robots,
        mo, pl,
    )


@app.cell
def _(HistoryQueries, LakePaths, config, load_report_config, load_robots):
    lake = LakePaths(config.lake_root(None))  # $FLASHPOINT_LAKE or ~/flashpoint-lake
    queries = HistoryQueries(
        lake,
        load_robots(config.config_root() / "robots"),
        load_report_config(config.config_root()),
    )
    return lake, queries


@app.cell
def _(METRICS, mo, queries):
    values = queries.filter_values()
    metric = mo.ui.dropdown({m.label: k for k, m in METRICS.items()}, value="Peak temperature", label="Metric")
    event = mo.ui.dropdown(["(all)", *values["event"]], value="(all)", label="Event")
    subsystem = mo.ui.dropdown(["(all)", *values["subsystem"]], value="(all)", label="Subsystem")
    mo.hstack([metric, event, subsystem])
    return event, metric, subsystem


@app.cell
def _(Filters, event, metric, pl, queries, subsystem):
    filters = Filters(
        event=None if event.value == "(all)" else event.value,
        subsystem=None if subsystem.value == "(all)" else subsystem.value,
    )
    trend = queries.trend(metric.value, "unit", filters)
    points = pl.DataFrame(trend["points"]) if trend["points"] else pl.DataFrame()
    points
    return points, trend


@app.cell
def _(mo, points, trend):
    # One line per unit, one point per match (gaps where the unit wasn't installed).
    chart = (
        mo.ui.altair_chart(
            __import__("altair").Chart(points.to_pandas())
            .mark_line(point=True)
            .encode(x="t:T", y="value:Q", color="series:N", tooltip=["match_key", "series", "value"])
            .properties(title=f"{trend['label']} ({trend['unit']})")
        )
        if not points.is_empty()
        else mo.md("No matches in range.")
    )
    chart
    return


@app.cell
def _(mo, points):
    units = sorted(points["series"].unique().to_list()) if not points.is_empty() else []
    unit = mo.ui.dropdown(units, value=units[0] if units else None, label="Unit")
    unit
    return (unit,)


@app.cell
def _(mo, pl, queries, unit):
    detail = queries.unit(unit.value) if unit.value else None
    mo.vstack(
        [
            mo.md(f"### {unit.value}"),
            mo.ui.table(pl.DataFrame([detail["totals"]])),
            mo.ui.table(pl.DataFrame(detail["lifeline"])),
        ]
    ) if detail and detail["found"] else mo.md("Pick a unit.")
    return


if __name__ == "__main__":
    app.run()
