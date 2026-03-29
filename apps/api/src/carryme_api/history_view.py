"""History ranking and dashboard rendering helpers."""

from __future__ import annotations

from html import escape

from carryme_models import OpportunityRecord
from carryme_runtime import filter_candidate_records


def _record_identity_key(record: OpportunityRecord) -> str:
    """Build a stable identity key for configured pairs."""

    if record.pair.label:
        return record.pair.label
    return "|".join(
        [
            record.pair.left_venue,
            record.pair.left_symbol,
            record.pair.left_fee_profile,
            record.pair.right_venue,
            record.pair.right_symbol,
            record.pair.right_fee_profile,
        ]
    )


def _display_label(record: OpportunityRecord) -> str:
    """Render a readable label for configured and unlabeled pairs."""

    if record.pair.label:
        return record.pair.label
    return (
        f"{record.pair.left_venue}:{record.pair.left_symbol}"
        f" ↔ {record.pair.right_venue}:{record.pair.right_symbol}"
    )


def _stale_sort_value(record: OpportunityRecord) -> float:
    """Prefer non-stale books when ranking saved opportunities."""

    is_stale = bool(record.opportunity.short_stale_book) or bool(record.opportunity.long_stale_book)
    return 0.0 if is_stale else 1.0


def _min_metric(left: float | None, right: float | None) -> float:
    """Return the weaker side of a two-venue liquidity metric."""

    values = [value for value in (left, right) if value is not None]
    if not values:
        return 0.0
    return min(values)


def _spread_sort_value(record: OpportunityRecord) -> float:
    """Prefer tighter combined spreads when ranking opportunities."""

    spreads = [
        spread
        for spread in (
            record.opportunity.short_spread_rate,
            record.opportunity.long_spread_rate,
        )
        if spread is not None
    ]
    if len(spreads) != 2:
        return float("-inf")
    return -sum(spreads)


def _break_even_sort_value(record: OpportunityRecord) -> float:
    """Prefer faster payback when entry edges tie."""

    if record.opportunity.break_even_days_entry is None:
        return float("-inf")
    return -record.opportunity.break_even_days_entry


def latest_records_by_label(
    records: list[OpportunityRecord],
    *,
    limit: int,
) -> list[OpportunityRecord]:
    """Return the latest record for each label, preserving recency order."""

    selected: list[OpportunityRecord] = []
    seen_labels: set[str] = set()
    for record in records:
        dedupe_key = _record_identity_key(record)
        if dedupe_key in seen_labels:
            continue
        selected.append(record)
        seen_labels.add(dedupe_key)
        if len(selected) >= limit:
            break
    return selected


def rank_history_records(
    records: list[OpportunityRecord],
    *,
    limit: int,
) -> list[OpportunityRecord]:
    """Rank history rows by most attractive one-day entry economics first."""

    ranked = sorted(
        records,
        key=lambda record: (
            record.opportunity.one_day_net_edge_after_entry,
            _stale_sort_value(record),
            record.opportunity.one_day_net_edge_after_round_trip,
            _min_metric(
                record.opportunity.short_open_interest,
                record.opportunity.long_open_interest,
            ),
            _min_metric(
                record.opportunity.short_daily_volume,
                record.opportunity.long_daily_volume,
            ),
            _spread_sort_value(record),
            (
                record.opportunity.capacity.max_entry_notional
                if record.opportunity.capacity
                and record.opportunity.capacity.max_entry_notional is not None
                else -1.0
            ),
            _break_even_sort_value(record),
            record.recorded_at.timestamp(),
        ),
        reverse=True,
    )
    return ranked[:limit]


def render_dashboard(records: list[OpportunityRecord]) -> str:
    """Render a simple operator dashboard over saved history."""

    latest = latest_records_by_label(records, limit=20)
    ranked = rank_history_records(latest, limit=20)
    total_rows = len(records)
    tracked_labels = len({_record_identity_key(record) for record in records})
    best_entry_edge = ranked[0].opportunity.one_day_net_edge_after_entry if ranked else 0.0

    rows = "\n".join(_render_row(record) for record in ranked)
    if not rows:
        rows = '<tr><td colspan="8">No persisted opportunity history yet.</td></tr>'

    subtitle = (
        "Persisted funding opportunities ranked by current one-day net "
        "entry edge with tie-breakers for stale books, exit economics, "
        "liquidity, spreads, and recency."
    )
    stats_markup = "\n".join(
        [
            _render_stat_card("Saved Rows", str(total_rows)),
            _render_stat_card("Tracked Labels", str(tracked_labels)),
            _render_stat_card("Best Entry Edge", f"{best_entry_edge:.6f}"),
        ]
    )

    return _render_dashboard_document(
        html_title="carryme dashboard",
        heading="carryme operator view",
        subtitle=subtitle,
        stats_markup=stats_markup,
        rows=rows,
        background="radial-gradient(circle at top left, #fff7dd, #f6f4ed)",
        bg="#f6f4ed",
        panel="#fffdf8",
        ink="#1a1f16",
        muted="#647066",
        border="#d7d2c5",
    )


def render_candidate_dashboard(
    records: list[OpportunityRecord],
    *,
    min_one_day_net_edge_after_entry: float | None,
    min_capacity_notional: float | None,
) -> str:
    """Render a filtered dashboard focused on candidate opportunities."""

    latest = latest_records_by_label(records, limit=200)
    candidates = filter_candidate_records(
        latest,
        min_one_day_net_edge_after_entry=min_one_day_net_edge_after_entry,
        min_capacity_notional=min_capacity_notional,
    )
    ranked = rank_history_records(candidates, limit=20)
    rows = "\n".join(_render_row(record) for record in ranked)
    if not rows:
        rows = (
            '<tr><td colspan="8">No candidate opportunities match the current thresholds.</td></tr>'
        )

    subtitle = (
        "Filtered candidates from the latest saved row per label using explicit "
        "minimum one-day entry edge and capacity thresholds."
    )
    stats_markup = "\n".join(
        [
            _render_stat_card("Candidates", str(len(candidates))),
            _render_stat_card(
                "Min Entry Edge",
                (
                    "-"
                    if min_one_day_net_edge_after_entry is None
                    else f"{min_one_day_net_edge_after_entry:.6f}"
                ),
            ),
            _render_stat_card(
                "Min Capacity",
                "-" if min_capacity_notional is None else f"{min_capacity_notional:.2f}",
            ),
        ]
    )

    return _render_dashboard_document(
        html_title="carryme candidates",
        heading="carryme candidate view",
        subtitle=subtitle,
        stats_markup=stats_markup,
        rows=rows,
        background="linear-gradient(135deg, #f7fce6, #eff4ef)",
        bg="#eff4ef",
        panel="#fefef9",
        ink="#0f1720",
        muted="#5e6e60",
        border="#c9d6c8",
    )


def _render_dashboard_document(
    *,
    html_title: str,
    heading: str,
    subtitle: str,
    stats_markup: str,
    rows: str,
    background: str,
    bg: str,
    panel: str,
    ink: str,
    muted: str,
    border: str,
) -> str:
    """Render a themed HTML dashboard document."""

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{html_title}</title>
    <style>
      :root {{
        color-scheme: light;
        --bg: {bg};
        --panel: {panel};
        --ink: {ink};
        --muted: {muted};
        --border: {border};
      }}
      body {{
        margin: 0;
        font-family: "Iowan Old Style", "Palatino Linotype", serif;
        background: {background};
        color: var(--ink);
      }}
      main {{
        max-width: 1120px;
        margin: 0 auto;
        padding: 32px 20px 64px;
      }}
      h1 {{
        margin: 0 0 8px;
        font-size: 2.6rem;
      }}
      p {{
        color: var(--muted);
        max-width: 760px;
      }}
      .stats {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 12px;
        margin: 24px 0;
      }}
      .card {{
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 16px;
      }}
      .label {{
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: var(--muted);
      }}
      .value {{
        font-size: 1.6rem;
        margin-top: 6px;
      }}
      table {{
        width: 100%;
        border-collapse: collapse;
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 14px;
        overflow: hidden;
      }}
      th, td {{
        padding: 12px 10px;
        border-bottom: 1px solid var(--border);
        text-align: left;
        vertical-align: top;
      }}
      th {{
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: var(--muted);
      }}
      tr:last-child td {{
        border-bottom: none;
      }}
    </style>
  </head>
  <body>
    <main>
      <h1>{heading}</h1>
      <p>{subtitle}</p>
      <section class="stats">
        {stats_markup}
      </section>
      <table>
        <thead>
          <tr>
            <th>Label</th>
            <th>Symbol</th>
            <th>Short</th>
            <th>Long</th>
            <th>Entry Edge</th>
            <th>Round Trip</th>
            <th>Break-even Days</th>
            <th>Recorded</th>
          </tr>
        </thead>
        <tbody>
          {rows}
        </tbody>
      </table>
    </main>
  </body>
</html>"""


def _render_row(record: OpportunityRecord) -> str:
    label = escape(_display_label(record))
    symbol = escape(record.opportunity.canonical_symbol)
    short_venue = escape(record.opportunity.short_venue)
    long_venue = escape(record.opportunity.long_venue)
    entry_edge = f"{record.opportunity.one_day_net_edge_after_entry:.6f}"
    round_trip_edge = f"{record.opportunity.one_day_net_edge_after_round_trip:.6f}"
    break_even = (
        f"{record.opportunity.break_even_days_entry:.3f}"
        if record.opportunity.break_even_days_entry is not None
        else "-"
    )
    recorded_at = escape(record.recorded_at.isoformat())
    return (
        "<tr>"
        f"<td>{label}</td>"
        f"<td>{symbol}</td>"
        f"<td>{short_venue}</td>"
        f"<td>{long_venue}</td>"
        f"<td>{entry_edge}</td>"
        f"<td>{round_trip_edge}</td>"
        f"<td>{break_even}</td>"
        f"<td>{recorded_at}</td>"
        "</tr>"
    )


def _render_stat_card(label: str, value: str) -> str:
    safe_label = escape(label)
    safe_value = escape(value)
    return (
        '<div class="card">'
        f'<div class="label">{safe_label}</div>'
        f'<div class="value">{safe_value}</div>'
        "</div>"
    )
