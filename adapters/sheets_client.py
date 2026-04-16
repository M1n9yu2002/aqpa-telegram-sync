from __future__ import annotations

from typing import Any

import gspread

from configs.settings import get_settings


class SheetsClientError(Exception):
    pass


class SheetsClient:
    def __init__(self) -> None:
        self.settings = get_settings()

    def _open_spreadsheet(self) -> gspread.Spreadsheet:
        try:
            client = gspread.service_account(
                filename=str(self.settings.google_service_account_path)
            )
            return client.open_by_key(self.settings.google_sheet_id)
        except Exception as exc:
            raise SheetsClientError(
                "Failed to access Google Sheets with the configured service account."
            ) from exc

    def _read_worksheet_records(self, worksheet_name: str) -> list[dict[str, Any]]:
        spreadsheet = self._open_spreadsheet()
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound as exc:
            raise SheetsClientError(
                f"Worksheet '{worksheet_name}' was not found in the configured spreadsheet."
            ) from exc
        except Exception as exc:
            raise SheetsClientError(
                f"Failed to open worksheet '{worksheet_name}'."
            ) from exc

        try:
            values = worksheet.get_all_values()
        except Exception as exc:
            raise SheetsClientError(
                f"Failed to read rows from worksheet '{worksheet_name}'."
            ) from exc

        if not values:
            return []

        headers = values[0]
        records: list[dict[str, Any]] = []

        for row_values in values[1:]:
            padded_row = row_values + [""] * (len(headers) - len(row_values))
            record = {
                header: padded_row[index]
                for index, header in enumerate(headers)
            }
            records.append(record)

        return records

    def read_positions(self) -> list[dict[str, Any]]:
        return self._read_worksheet_records("Positions")

    def read_watchlist(self) -> list[dict[str, Any]]:
        return self._read_worksheet_records("Watchlist")
