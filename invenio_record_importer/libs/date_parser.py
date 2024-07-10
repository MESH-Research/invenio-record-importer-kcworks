import re
from datetime import datetime
from typing import List
import arrow
import dateparser
from invenio_record_importer.utils import valid_date, monthwords, seasonwords
from flask import current_app as app
import timefhuman


class DateParser:
    """A class for parsing dirty human-readable date strings.

    The main method is `repair_date`, which attempts to repair a date string
    by applying a series of date parsing functions. The functions are applied
    in the following order:

    1. `restore_2digit_year`: Expand 2-digit years to 4-digit years
    2. `reorder_date_parts`: Reorder the parts of a date string
    3. `convert_date_words`: Convert date words to numbers
    4. `parse_human_readable`: Parse a human readable date string

    If the date string cannot be repaired as a single date, the method
    in turn attempts to repair the date as a date range using the
    `repair_range` method. The `repair_range` method applies the same
    date parsing functions as `repair_date` to each part of the date range.
    It also invokes additional logic to handle date ranges with ambiguous
    years and various human-readable formats for expressing ranges.

    The class also includes a number of helper methods for parsing and
    manipulating date strings, such as `fill_missing_zeros`,
    `reorder_date_parts`, etc. All of these are static methods and can be
    used independently of the `repair_date` and `repair_range` methods.
    """

    def __init__(self):
        pass

    @staticmethod
    def convert_date_words(date: str) -> str:
        """Convert a date string with words to a date string with numbers.

        Treats season names as the first month of the season, and month names
        and abbreviations as the month number. Attempts to identify a 4-digit
        year and a day number. Reorders the date parts to YYYY-MM-DD or YYYY-MM
        or YYYY format.

        Args:
            date (str): A date string with words

        Returns:
            str: The date string with words converted to numbers
        """
        date_parts = re.split(r"[, \.]", date)
        date_parts = [p for p in date_parts if p not in ["del", "de", " ", ""]]
        # print(date_parts)
        month = ""
        day = ""
        year = ""
        month_abbrevs = {
            "jan": "01",
            "janv": "01",
            "ja": "01",
            "ene": "01",
            "feb": "02",
            "febr": "02",
            "fe": "02",
            "fev": "02",
            "mar": "03",
            "apr": "04",
            "ap": "04",
            "may": "05",
            "jun": "06",
            "jul": "07",
            "aug": "08",
            "au": "08",
            "augus": "08",
            "sep": "09",
            "sept": "09",
            "septmber": "09",
            "se": "09",
            "oct": "10",
            "octo": "10",
            "oc": "10",
            "nov": "11",
            "no": "11",
            "dec": "12",
            "de": "12",
            "dez": "12",
        }
        season_abbrevs = {
            "spr": "03",
            "sum": "06",
            "fal": "09",
            "win": "12",
        }
        # try to identify month words and abbreviations
        # and convert to numbers
        for d in date_parts:
            d_cand = d.lower().strip().replace(",", "").replace(".", "")
            if d_cand in monthwords.keys():
                month = monthwords[d_cand]
                date_parts = [p for p in date_parts if p != d]
            elif d_cand in month_abbrevs.keys():
                month = month_abbrevs[d_cand]
                date_parts = [p for p in date_parts if p != d]
            elif d_cand in seasonwords.keys():
                month = seasonwords[d_cand].split("-")[0]
                date_parts = [p for p in date_parts if p != d]
            elif d_cand in season_abbrevs.keys():
                month = season_abbrevs[d_cand].split("-")[0]
                date_parts = [p for p in date_parts if p != d]
        # try to identify a 4-digit year
        for d in date_parts:
            if len(d) == 4 and d.isdigit() and int(d) > 0 < 3000:
                year = d
                date_parts = [p for p in date_parts if p != d]
        # try to identify a day from remaining parts
        for d in date_parts:
            suffixes = r"(th|st|nd|rd)"
            day_cand = re.sub(suffixes, "", d.lower())
            if day_cand.isdigit() and len(day_cand) <= 2:
                day = day_cand
                date_parts = [p for p in date_parts if p != d]
        # if we have identified year and month, assume remaining part is day
        if year and month and not day and len(date_parts) == 1:
            day = date_parts[0]
        # reorder parts to YYYY-MM-DD or YYYY-MM or YYYY
        if year and month and day:
            return DateParser.reorder_date_parts("-".join([year, month, day]))
        elif year and month:
            return "-".join([year, month])
        else:
            return date

    @staticmethod
    def fill_missing_zeros(date: str) -> str:
        """Fill in missing zeros in a date string.

        Args:
            date (str): A date string with missing zeros

        Returns:
            str: The date string with missing zeros filled in
        """
        date_parts = list(filter(None, re.split(r"[\.\-:\/,]+", date)))
        for i, part in enumerate(date_parts):
            if len(part) < 2:
                date_parts[i] = "0" + part
            if part.isdigit() and len(part) == 2 and int(part) > 31:
                if int(part) <= 27:
                    date_parts[i] = "20" + part
                else:
                    date_parts[i] = "19" + part
        return "-".join(date_parts)

    @staticmethod
    def reorder_date_parts(date: str) -> str:
        """Reorder the parts of a date string.

        Args:
            date (str): A date string with parts in the order YYYY-DD-MM or
                        DD-MM-YYYY or MM-DD-YYYY

        Returns:
            str: The date string with parts in the order YYYY-MM-DD
        """
        date = DateParser.fill_missing_zeros(date)
        date_parts = list(filter(None, re.split(r"[\.\-:\/ ]+", date)))
        # print(date_parts)
        try:
            assert len(date_parts) >= 2 <= 3
            assert [int(p) for p in date_parts]
            year = [d for d in date_parts if len(d) == 4][0]
            others = [d for d in date_parts if len(d) != 4]
            if len(others) == 2:
                month_candidates = [d for d in date_parts if int(d) <= 12]
                if len(month_candidates) == 1:
                    month = month_candidates[0]
                    day = [d for d in others if d != month][0]
                else:
                    month, day = others
                return "-".join([year, month, day])
            else:
                return "-".join([year, others[0]])
        except (AssertionError, IndexError, ValueError):
            return date

    @staticmethod
    def parse_human_readable(date: str) -> str:
        """Parse a human readable date string.

        Args:
            date (str): A human readable date string

        Returns:
            str: The date string parsed into ISO format
        """
        try:
            return timefhuman(date).isoformat().split("T")[0]
        except (
            ValueError,
            TypeError,
            IndexError,
            AssertionError,
            AttributeError,
        ):
            return date

    @staticmethod
    def is_seasonal_date(date: str) -> bool:
        """Return True if the date is a human readable seasonal date."""
        seasons = [
            "spring",
            "summer",
            "fall",
            "winter",
            "autumn",
            "spr",
            "sum",
            "fal",
            "win",
            "aut",
            "intersession",
        ]
        date_parts = date.split(" ")
        valid = False
        if len(date_parts) == 2:
            years = [d for d in date_parts if len(d) in [2, 4] and d.isdigit()]
            if len(years) == 1:
                season_part = [d for d in date_parts if d != years[0]][0]
                season_parts = re.findall(r"\w+", season_part)
                if all(s for s in season_parts if s.lower() in seasons):
                    valid = True
        return valid

    @staticmethod
    def restore_2digit_year(date: str) -> str:
        pattern = r"^\d{2}[/,-\.]\d{2}[/,-\.]\d{2}$"
        if re.match(pattern, date):
            date = arrow.get(dateparser.parse(date)).date().isoformat()
        return date

    @staticmethod
    def repair_date(date: str, id: str = "") -> tuple[bool, str]:
        print_id = "hc:16967"
        if id == print_id:
            print(f"repairing date: {date}")
        invalid = True
        newdate = date
        for date_func in [
            DateParser.restore_2digit_year,
            DateParser.reorder_date_parts,
            DateParser.convert_date_words,
            DateParser.parse_human_readable,
        ]:
            newdate = date_func(date)
            if id == print_id:
                print(date_func)
                print(newdate)
            if valid_date(newdate):
                invalid = False
                break
        # if invalid and is_seasonal_date(date):
        #     invalid = False

        return invalid, newdate

    @staticmethod
    def extract_year(s):
        match = re.search(r"\b(19|20)\d{2}\b", s)
        if match:
            return match.group(0)
        return None

    @staticmethod
    def repair_range(date: str, id: str = "") -> tuple[bool, str]:
        """Attempt to repair a date range.

        Usage examples:

        >>> DateParser.repair_range("2019-2020")
        (False, '2019/2020')

        >>> DateParser.repair_range("2019 to 2020")
        (False, '2019/2020')

        >>> DateParser.repair_range("2019/2020")
        (False, '2019/2020')

        >>> DateParser.repair_range("2019-2020-2021")
        (True, '2019-2020-2021')

        >>> DateParser.repair_range("Jan-Feb 2019")
        (False, '2019-01/2019-02')

        >>> DateParser.repair_range("Winter 2019")
        (False, '2019-12')

        >>> DateParser.repair_range("Winter 2019-2020")
        (False, '2019-12/2020-02')

        >>> DateParser.repair_range("Winter/Spring 2019/2020")
        (False, '2019-12/2020-03')

        >>> DateParser.repair_range("Jul-dez/2019")
        (False, '2019-07/2019-12')
        """
        # print("repairing range")
        print_id = "hc:16967"
        invalid = True
        raw_range_parts = re.split(r"[\-–\/]", date)
        range_parts = [*raw_range_parts]
        # handle dates like "winter/fall 2019/2020"
        if len(range_parts) == 3 and re.match(
            r"\s(19|20)\d{2}[\-\/](19|20)\d{2}", date[-10:]
        ):
            range_parts = [
                f"{range_parts[0]} {range_parts[1][-4:]}",
                f"{range_parts[1][:-4]} {range_parts[2]}",
            ]
        # handle dates like "2019 to 2020"
        if len(range_parts) == 1:
            range_parts = re.split(r" to ", date)
        if id == print_id:
            print(f"range_parts: {range_parts}")
        # expand 2-digit years to 4-digit years
        if (
            len(range_parts) == 2
            and len(range_parts[0]) == 4
            and range_parts[0][:2] in ["19", "20"]
            and len(range_parts[1]) == 2
        ):
            if range_parts[1].isdigit() and int(range_parts[1]) <= 30:
                range_parts[1] = "20" + range_parts[1]
            elif range_parts[1].isdigit() and int(range_parts[1]) > 30:
                range_parts[1] = "19" + range_parts[1]
        global_years = [
            DateParser.extract_year(part)
            for part in range_parts
            if DateParser.extract_year(part)
        ]
        for i, part in enumerate(range_parts):
            if id == print_id:
                print(f"part: {part}")
            if not valid_date(part):
                # handle dates like "winter/spring 2019"
                if not DateParser.extract_year(part) and len(global_years) > 0:
                    invalid, repaired = DateParser.repair_date(
                        part + " " + global_years[0]
                    )
                else:
                    invalid, repaired = DateParser.repair_date(part)
                range_parts[i] = repaired
                if id == "hc:16967":
                    print(f"repaired: {repaired}")
            if not valid_date(range_parts[i]):
                invalid = True
            else:
                invalid = False
            if id == "hc:16967":
                print(f"range_parts[i]: {range_parts[i]}")
        # print(range_parts)
        if not invalid:
            if range_parts[0] > range_parts[1]:
                app.logger.debug(f"invalid date range from {raw_range_parts}")
                # handle winter dates where year is ambiguous
                if any(
                    [
                        p
                        for p in ["Winter", "winter"]
                        if p in raw_range_parts[0]
                    ]
                ):
                    range_parts[0] = (
                        f"{str(int(range_parts[0][:4]) - 1)}{range_parts[0][4:]}"
                    )
                    app.logger.debug(f"adjusted winter date: {range_parts[0]}")
                else:
                    app.logger.debug(
                        f"failed repairing invalid: {range_parts}"
                    )
                    invalid = True
            date = "/".join(range_parts)
        # print(date)
        return invalid, date
