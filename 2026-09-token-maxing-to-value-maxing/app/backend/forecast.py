"""Lightweight cost forecast (app-side replica of the dashboard's AI_FORECAST).

The dashboard's Cost Forecast page uses Databricks `AI_FORECAST(...)` over the
daily-cost series. The app reads Lakebase (no warehouse), so we replicate the
shape with an ordinary least-squares linear projection plus a ±1.96σ band —
enough to drive the "historical + projected + confidence band" chart and the
spend/run-rate/growth KPIs, and it stays scope-aware (fed the caller's series).
"""
import statistics
from datetime import date, timedelta


def linear_forecast(dates, costs, horizon=30):
    """dates: sorted 'YYYY-MM-DD'; costs: parallel floats. Returns
    (forecast_dates, predictions, band) for the next `horizon` days."""
    n = len(costs)
    if n == 0:
        return [], [], 0.0
    xs = list(range(n))
    mx = sum(xs) / n
    my = sum(costs) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    sxy = sum((xs[i] - mx) * (costs[i] - my) for i in range(n))
    slope = sxy / sxx if sxx else 0.0
    intercept = my - slope * mx
    resid = [costs[i] - (intercept + slope * xs[i]) for i in range(n)]
    sd = statistics.pstdev(resid) if n > 1 else 0.0
    band = round(1.96 * sd, 2)

    last = date.fromisoformat(str(dates[-1])[:10])
    fdates, preds = [], []
    for i in range(1, horizon + 1):
        yhat = max(0.0, intercept + slope * (n - 1 + i))
        fdates.append((last + timedelta(days=i)).isoformat())
        preds.append(round(yhat, 2))
    return fdates, preds, band
