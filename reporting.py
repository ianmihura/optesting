import pandas as pd
from datetime import date
from typing import List, Dict


class PerformanceTracker:
    """
    Records daily portfolio values and realized PnLs from trades to generate
    performance metrics such as Drawdown, Win/Loss ratios, etc.
    """

    def __init__(self):
        self.daily_values: List[Dict] = []
        self.trade_log: List[Dict] = []

    def log_daily_value(self, current_date: date, value: float):
        self.daily_values.append({"date": current_date, "value": value})

    def log_trade(
        self,
        current_date: date,
        symbol: str,
        was_long: bool,
        realized_pnl: float,
        details: str = "",
    ):
        self.trade_log.append(
            {
                "date": current_date,
                "symbol": symbol,
                "type": "LONG" if was_long else "SHORT",
                "pnl": realized_pnl,
                "details": details,
            }
        )

    def generate_report(self) -> str:
        if not self.daily_values:
            return "No data logged."

        df_daily = pd.DataFrame(self.daily_values)
        if "value" not in df_daily.columns or df_daily.empty:
            return "No portfolio values to report."

        # Compute Drawdowns
        df_daily["peak"] = df_daily["value"].cummax()
        df_daily["drawdown"] = (df_daily["peak"] - df_daily["value"]) / df_daily["peak"]
        df_daily["drawdown_pct"] = df_daily["drawdown"] * 100.0

        max_dd_pct = df_daily["drawdown_pct"].max()

        # Longest Drawdown Duration
        in_drawdown = df_daily["drawdown"] > 0
        df_daily["dd_streak"] = in_drawdown.groupby((~in_drawdown).cumsum()).cumcount()
        longest_dd_days = df_daily["dd_streak"].max()

        # Trades metrics
        trades_text = ""
        if self.trade_log:
            df_trades = pd.DataFrame(self.trade_log)
            total_trades = len(df_trades)
            winners = df_trades[df_trades["pnl"] > 0]
            losers = df_trades[df_trades["pnl"] <= 0]

            win_rate = (len(winners) / total_trades) * 100.0 if total_trades > 0 else 0
            loss_rate = (len(losers) / total_trades) * 100.0 if total_trades > 0 else 0

            avg_win = winners["pnl"].mean() if not winners.empty else 0.0
            max_win = winners["pnl"].max() if not winners.empty else 0.0

            avg_loss = losers["pnl"].mean() if not losers.empty else 0.0
            max_loss = losers["pnl"].min() if not losers.empty else 0.0

            trades_text = (
                f"Total Completed Round-Trips: {total_trades}\n"
                f"Win Rate: {win_rate:.2f}% | Loss Rate: {loss_rate:.2f}%\n"
                f"Avg Win: ${avg_win:.2f} | Max Win: ${max_win:.2f}\n"
                f"Avg Loss: ${avg_loss:.2f} | Max Loss: ${max_loss:.2f}\n"
            )
        else:
            trades_text = "No completed trades to report.\n"

        start_val = df_daily["value"].iloc[0]
        end_val = df_daily["value"].iloc[-1]
        total_return = (
            ((end_val - start_val) / start_val) * 100.0 if start_val > 0 else 0
        )

        start_date = df_daily["date"].min()
        end_date = df_daily["date"].max()
        duration = end_date - start_date
        step_size = df_daily["date"].diff().median() if len(df_daily) > 1 else "N/A"

        report = (
            f"--- Backtest Performance Report ---\n"
            f"Start Date:    {start_date}\n"
            f"End Date:      {end_date}\n"
            f"Duration:      {duration}\n"
            f"Step Size:     {step_size}\n"
            f"Initial Value: ${start_val:.2f}\n"
            f"Final Value:   ${end_val:.2f}\n"
            f"Total Return:  {total_return:.2f}%\n"
            f"Max Drawdown:  {max_dd_pct:.2f}%\n"
            f"Longest Drawdown Duration: {longest_dd_days} days\n"
            f"\n--- Trade Statistics ---\n"
            f"{trades_text}"
        )

        return report
