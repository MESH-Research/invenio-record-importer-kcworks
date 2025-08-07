import glob
import re
from typing import List

import arrow
import dateparser
import regex
import timefhuman
from flask import current_app as app
from invenio_record_importer_kcworks.utils import (
    monthwords,
    seasonwords,
    valid_date,
)


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

    Seasons are treated as the first month of the season. If a date string
    contains a season name, the method will attempt to identify the season
    and convert it to the first month of the season. If the date string
    contains a month name or abbreviation, the method will attempt to
    identify the month and convert it to the month number. If the date
    string contains a 4-digit year, the method will attempt to identify
    the year.

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
        date_parts = re.split(r"[, \.\/-]", date)
        if len(date_parts) == 1:
            date_parts = DateParser.split_mashed_datestring(date)
        date_parts = [
            p
            for p in date_parts
            if p.strip() not in ["del", "de", "of", "di", " ", ""]
        ]
        if (
            len(date_parts) <= 3
            and len([d for d in date_parts if regex.match(r"\p{L}+", d)]) <= 1
        ):
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
                    if not year:
                        year = d
                        date_parts = [p for p in date_parts if p != d]
                    # Fail if there are multiple 4-digit years
                    # and another part is a month or day
                    elif len([d for d in [day, month, *date_parts] if d]) >= 2:
                        return date
            # try to identify a day by looking at suffixes
            for d in date_parts:
                suffixes = r"(th|st|nd|rd|e)"
                day_cand = re.sub(suffixes, "", d.lower())
                if day_cand.isdigit() and len(day_cand) <= 2:
                    day = day_cand
                    date_parts = [p for p in date_parts if p != d]
            # if we have identified year and day, assume remaining part is month
            if day and year and not month and len(date_parts) == 1:
                month = date_parts[0]
            # if we have identified year and month, assume remaining part is day
            if year and month and not day and len(date_parts) == 1:
                day = date_parts[0]
            # if we have identified only year and only one other part
            # assume the other part is the month
            if year and not month and not day and len(date_parts) == 1:
                month = date_parts[0]
            # if we have not identified month or day, and there are two parts left
            # assume the first part is the month and the second part is the day
            if year and not month and not day and len(date_parts) == 2:
                month = date_parts[0]
                day = date_parts[1]
            # reorder parts to YYYY-MM-DD or YYYY-MM or YYYY
            if year and month and day:
                return DateParser.reorder_date_parts(
                    "-".join([year, month, day])
                )
            elif year and month:
                return "-".join([year, month])
            else:
                return date
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
    def fill_2digit_year(year: str) -> str:
        """Fill in missing zeros in a 2-digit year string.

        Args:
            year (str): A 2-digit year string

        Returns:
            str: The year string with missing zeros filled in
        """
        if len(year) == 2 and year.isdigit():
            if int(year) <= 30:
                return "20" + year
            else:
                return "19" + year
        return year

    @staticmethod
    def reorder_date_parts(date: str) -> str:
        """Reorder the parts of a date string.

        If the date string has a two-digit year, expand it to a four-digit year.
        This assumes that the year is the last part of the date string.

        If two parts of the date string are numbers less than 13, assume they
        are the month and day, and assume that the month comes before the day.

        Args:
            date (str): A date string with parts in the order YYYY-DD-MM or
                        DD-MM-YYYY or MM-DD-YYYY

        Returns:
            str: The date string with parts in the order YYYY-MM-DD
        """
        date = DateParser.fill_missing_zeros(date)
        date_parts = list(filter(None, re.split(r"[\.\-:\/ ]+", date)))
        if len(date_parts) == 1:
            date_parts = DateParser.split_mashed_datestring(date)
        # print(date_parts)
        try:
            assert len(date_parts) >= 2 <= 3
            assert [int(p) for p in date_parts]
            year = None
            day = None
            month = None
            others = []
            try:
                year = [d for d in date_parts if len(d) == 4][0]
                year_index = date_parts.index(year)
                others = [
                    d for idx, d in enumerate(date_parts) if year_index != idx
                ]
            except IndexError:
                if len(date_parts[-1]) == 2:
                    if int(date_parts[-1]) <= arrow.now().year % 100:
                        year = "20" + date_parts[-1]
                    else:
                        year = "19" + date_parts[-1]
                    others = date_parts[:-1]
                else:
                    raise ValueError
            if len(others) == 2:
                month_candidates = [d for d in date_parts if int(d) <= 12]
                if len(month_candidates) == 1:
                    month = month_candidates[0]
                    day = [d for d in others if d != month][0]
                else:
                    month, day = others
                return "-".join([year, month, day])
            elif (
                not month
                and not day
                and len(others) == 1
                and re.match(r"\d{4}", others[0])
            ):
                return "-".join([year, others[0][:2], others[0][2:]])
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
    def remove_stray_parentheses(date: str) -> str:
        return re.sub(r"\(|\)", "", date)

    @staticmethod
    def split_mashed_datestring(date: str) -> List[str]:
        """Divide a datestring lacking delimiters into parts.

        Works for entirely numeric datestrings, e.g. "20210405".
        Also works for datestrings with a year and month, e.g. "202104".
        Works for datestrings combining words and numbers, e.g. "April2021"
        or "20April2021". In all cases there must be a 4-digit year at the
        beginning or end of the string.

        :param date: A datestring
        :return: A list of date parts
        """
        parts = [date]
        if re.match(r"\d{8}", date):
            if re.match(r"(19|20)\d{2}\d{2}", date) and not re.match(
                r"\d{2}\d{2}(19|20)", date
            ):
                parts = [date[:4], date[4:6], date[6:]]
            elif re.match(r"\d{2}\d{2}(19|20)", date):
                parts = [date[:2], date[2:4], date[4:]]
        elif regex.match(r"\d{4}\p{L}+\d\d?", date) and (
            date[-2:] <= "31" or date[-1:] <= "9"
        ):
            if date[-2:].isdigit():
                parts = [date[:4], date[4:-2], date[-2:]]
            else:
                parts = [date[:4], date[4:-1], date[-1:]]
        elif regex.match(r"\d\d?\p{L}+\d{4}", date) and (
            date[:2] <= "31" or date[:1] <= "9"
        ):
            if date[:2].isdigit():
                parts = [date[:2], date[2:-4], date[-4:]]
            else:
                parts = [date[:1], date[1:-4], date[-4:]]
        elif regex.match(r"\d{4}\p{L}+", date):
            parts = [date[:4], date[4:]]
        elif regex.match(r"\p{L}+\d{4}", date):
            parts = [date[:-4], date[-4:]]
        return parts

    @staticmethod
    def repair_date(date: str, id: str = "") -> tuple[bool, str]:
        """Convert a human readable date string to EDTF format if possible.

        Usage examples:

        >>> DateParser.repair_date(" 2021")
        (False, '2021')

        >>> DateParser.repair_date("2021)")
        (False, '2021')

        >>> DateParser.repair_date("2019-")
        (False, '2019')

        FIXME: should the above fail because of the trailing hyphen?

        >>> DateParser.repair_date("jun, 2019")
        (False, '2019-06')

        >>> DateParser.repair_date("Jan. 2019")
        (False, '2019-01')

        >>> DateParser.repair_date("02.2019")
        (False, '2019-02')

        >>> DateParser.repair_date("January 21, 2019")
        (False, '2019-01-21')

        >>> DateParser.repair_date("2/18")
        (False, '2018-02')

        >>> DateParser.repair_date("21 january 2019")
        (False, '2019-01-21')

        >>> DateParser.repair_date("21 Janvier 2019")
        (False, '2019-01-21')

        >>> DateParser.repair_date("2019-01-21")
        (False, '2019-01-21')

        >>> DateParser.repair_date("21 JAN 2019")
        (False, '2019-01-21')

        >>> DateParser.repair_date("Winter 2019")
        (False, '2019-12')

        >>> DateParser.repair_date("Jan-21/2019")
        (False, '2019-01-21')

        >>> DateParser.repair_date("2017-4-24")
        (False, '2017-04-24')

        >>> DateParser.repair_date("2014-5-2")
        (False, '2014-05-02')

        >>> DateParser.repair_date("September 2021")
        (False, '2021-09')

        >>> DateParser.repair_date("04/08/2022")
        (False, '2022-04-08')

        >>> DateParser.repair_date("4/4/2022")
        (False, '2022-04-04')

        >>> DateParser.repair_date("October2022")
        (False, '2022-10')

        >>> DateParser.repair_date("02-2022")
        (False, '2022-02')

        >>> DateParser.repair_date("02-21-2017")
        (False, '2017-02-21')

        >>> DateParser.repair_date("03-01-2019")
        (False, '2019-03-01')

        >>> DateParser.repair_date("May 5th 2023")
        (False, '2023-05-05')

        >>> DateParser.repair_date("25th May, 2023")
        (False, '2023-05-25')

        >>> DateParser.repair_date("September 1st, 2023")
        (False, '2023-09-01')

        >>> DateParser.repair_date("20240404")
        (False, '2024-04-04')

        >>> DateParser.repair_date("06012018")
        (False, '2018-06-01')

        >>> DateParser.repair_date("5/2024")
        (False, '2024-05')

        >>> DateParser.repair_date("23. Dec 2011")
        (False, '2011-12-23')

        >>> DateParser.repair_date("09.02.2016")
        (False, '2016-09-02')

        >>> DateParser.repair_date("29/9/17")
        (False, '2017-09-29')

        >>> DateParser.repair_date("2015 (November 20)")
        (False, '2015-11-20')

        >>> DateParser.repair_date("2019 March 18")
        (False, '2019-03-18')

        >>> DateParser.repair_date("1st April 2019")
        (False, '2019-04-01')

        >>> DateParser.repair_date("09/ 2018.")
        (False, '2018-09')

        >>> DateParser.repair_date("28th of September 2018")
        (False, '2018-09-28')

        >>> DateParser.repair_date("November 1st, 2018.")
        (False, '2018-11-01')

        >>> DateParser.repair_date("11 de Diciembre de 2018")
        (False, '2018-12-11')

        >>> DateParser.repair_date("30Dec2018")
        (False, '2018-12-30')

        >>> DateParser.repair_date("Dec) 2015")
        (False, '2015-12')

        >>> DateParser.repair_date("2015 (Jul")
        (False, '2015-07')

        >>> DateParser.repair_date("31 Jan., 2020")
        (False, '2020-01-31')

        >>> DateParser.repair_date("13.05.2020.")
        (False, '2020-05-13')

        >>> DateParser.repair_date("1997, September")
        (False, '1997-09')

        >>> DateParser.repair_date("29-FEB-2020")
        (False, '2020-02-29')

        >>> DateParser.repair_date("08-Mar-2021")
        (False, '2021-03-08')

        >>> DateParser.repair_date("2022-0209")
        (False, '2022-02-09')

        >>> DateParser.repair_date("Oct.-2018")
        (False, '2018-10')

        >>> DateParser.repair_date("08/219")
        (True, '08/219')

        >>> DateParser.repair_date("jan20, 2019")
        (True, 'jan20, 2019')

        >>> DateParser.repair_date("22")
        (True, '22')

        >>> DateParser.repair_date("Spring semester 2019")
        (True, 'Spring semester 2019')

        >>> DateParser.repair_date("2019-30-30")
        (True, '2019-30-30')

        >>> DateParser.repair_date('אלול תשע\"ה')
        (True, 'אלול תשע\"ה')

        >>> DateParser.repair_date("September 2018 (Forthcoming)")
        (True, 'September 2018 Forthcoming')

        >>> DateParser.repair_date("25 (2019)")
        (True, '25 2019')

        >>> DateParser.repair_date("21. Juni 2018, korrig. Version")
        (True, '21. Juni 2018, korrig. Version')

        >>> DateParser.repair_date("27/0602018")
        (True, '27/0602018')

        >>> DateParser.repair_date("2018-07031")
        (True, '2018-07031')

        >>> DateParser.repair_date("07031 2018")
        (True, '07031 2018')

        >>> DateParser.repair_date("(vol. 2, no. 1), 2018")
        (True, 'vol. 2, no. 1, 2018')

        >>> DateParser.repair_date("Posted on September 18th, 2018")
        (True, 'Posted on September 18th, 2018')

        >>> DateParser.repair_date("2010 3.4: 485-487")
        (True, '2010 3.4: 485-487')

        >>> DateParser.repair_date("Forthcoming")
        (True, 'Forthcoming')

        >>> DateParser.repair_date("2009 (1st 2006)")
        (True, '2009 1st 2006')

        >>> DateParser.repair_date("38 000 BP")
        (True, '38 000 BP')

        >>> DateParser.repair_date("Meiji Period")
        (True, 'Meiji Period')

        >>> DateParser.repair_date("Published 2015, Revised 2019")
        (True, 'Published 2015, Revised 2019')

        >>> DateParser.repair_date("!3 February 2020")
        (True, '!3 February 2020')

        >>> DateParser.repair_date("206")
        (True, '206')

        >>> DateParser.repair_date("(2002) 2011")
        (True, '2002 2011')

        >>> DateParser.repair_date("AUGUS")
        (True, 'AUGUS')

        >>> DateParser.repair_date("10997")
        (True, '10997')

        >>> DateParser.repair_date("0602018")
        (True, '0602018')


        Usage examples (ranges that should fail):

        >>> DateParser.repair_date("2019-2020")
        (True, '2019-2020')

        >>> DateParser.repair_date("2019 - 2021")
        (True, '2019 - 2021')

        >>> DateParser.repair_date("2019 to 2020")
        (True, '2019 to 2020')

        >>> DateParser.repair_date("2019/2020")
        (True, '2019/2020')

        >>> DateParser.repair_date("Jan-Dec 19")
        (True, 'Jan-Dec 19')

        >>> DateParser.repair_date("Jan/December 2019")
        (True, 'Jan/December 2019')

        >>> DateParser.repair_date("2019-2020-2021")
        (True, '2019-2020-2021')

        >>> DateParser.repair_date("Jan-Feb 2019")
        (True, 'Jan-Feb 2019')

        >>> DateParser.repair_date("Winter 2019-2020")
        (True, 'Winter 2019-2020')

        >>> DateParser.repair_date("Winter/Spring 2019/2020")
        (True, 'Winter/Spring 2019/2020')

        >>> DateParser.repair_date("Fall/Winter 2011/2012")
        (True, 'Fall/Winter 2011/2012')

        >>> DateParser.repair_date("Spring / Summer 2020")
        (True, 'Spring / Summer 2020')

        >>> DateParser.repair_date("Winter-Spring 2013")
        (True, 'Winter-Spring 2013')

        >>> DateParser.repair_date("Jul-dez/2019")
        (True, 'Jul-dez/2019')

        >>> DateParser.repair_date("ene.-jun, 2012")
        (True, 'ene.-jun, 2012')

        >>> DateParser.repair_date("Jan - Feb 2016")
        (True, 'Jan - Feb 2016')

        >>> DateParser.repair_date("2015 (Jul-Dec)")
        (True, '2015 Jul-Dec')

        >>> DateParser.repair_date("2016 2019")
        (True, '2016 2019')

        """
        print_id = None
        if id == print_id:
            print(f"repairing date: {date}")
        invalid = True
        date = DateParser.remove_stray_parentheses(date.strip())
        if date and date[-1] == ".":
            date = date[:-1]
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
            if valid_date(newdate) and not re.match(
                r".*\d{4}[\.\s]+\d{4}.*", newdate
            ):
                invalid = False
                break

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

        >>> DateParser.repair_range("2019 - 2021")
        (False, '2019/2021')

        >>> DateParser.repair_range("2019 to 2020")
        (False, '2019/2020')

        >>> DateParser.repair_range("2019/2020")
        (False, '2019/2020')

        >>> DateParser.repair_range("Jan-Dec 19")
        (False, '2019-01/2019-12')

        >>> DateParser.repair_range("Jan/December 2019")
        (False, '2019-01/2019-12')

        >>> DateParser.repair_range("2019-2020-2021")
        (True, '2019-2020-2021')

        >>> DateParser.repair_range("Jan-Feb 2019")
        (False, '2019-01/2019-02')

        >>> DateParser.repair_range("Winter 2019-2020")
        (False, '2019-12/2020-02')

        >>> DateParser.repair_range("Winter/Spring 2019/2020")
        (False, '2019-12/2020-03')

        >>> DateParser.repair_range("Fall/Winter 2011/2012")
        (False, '2011-09/2012-12')

        >>> DateParser.repair_range("Spring / Summer 2020")
        (False, '2020-03/2020-06')

        >>> DateParser.repair_range("Winter-Spring 2013")
        (False, '2012-12/2013-03')

        >>> DateParser.repair_range("Jul-dez/2019")
        (False, '2019-07/2019-12')

        >>> DateParser.repair_range("ene.-jun, 2012")
        (False, '2012-01/2012-06')

        >>> DateParser.repair_range("Jan - Feb 2016")
        (False, '2016-01/2016-02')

        >>> DateParser.repair_range("2015 (Jul-Dec)")
        (False, '2015-07/2015-12')

        >>> DateParser.repair_range("08/219")
        (True, '08/219')

        >>> DateParser.repair_range("newsletter #99, February – March, 2005")
        (True, 'newsletter #99, February  2005/2005-03')

        >>> DateParser.repair_range("2016 (2019-2020)")
        (True, '2016 2019/2020')

        >>> DateParser.repair_range("2017 (2019")
        (True, '2017 (2019')

        >>> DateParser.repair_range("November/October 2020")
        (True, '2020-11/2020-10')

        >>> DateParser.repair_range("1573, 1585, 1613")
        (True, '1573, 1585, 1613')

        >>> DateParser.repair_range("2016, 2nd. corr. ed.")
        (True, '2016, 2nd. corr. ed.')

        """
        print_id = None
        invalid = True
        raw_range_parts = re.split(r"[\-–\/]", date)
        range_parts = [*raw_range_parts]
        # handle dates like "winter/fall 2019/2020"
        if len(range_parts) == 3 and re.match(
            r"\s(19|20)\d{2}?[\-\/](19|20)\d{2}?", date[-10:]
        ):
            range_parts = [
                f"{range_parts[0]} {range_parts[1][-4:]}",
                f"{range_parts[1][:-4]} {range_parts[2]}",
            ]
        elif (
            len(range_parts) == 3
            and re.match(r"(19|20)\d{2}?", range_parts[2])
            and not re.match(r"(19|20)\d{2}?", range_parts[1])
        ):
            range_parts = [
                range_parts[0],
                f"{range_parts[1]} {range_parts[2]}",
            ]
        # handle dates like "2019 to 2020"
        if len(range_parts) == 1:
            range_parts = re.split(r" to ", date)
        if id == print_id:
            print(f"range_parts: {range_parts}")
        # FIXME: expand 2-digit years to 4-digit years
        if len(range_parts) == 2:
            if (
                len(range_parts) == 2
                and len(range_parts[0]) == 4
                and range_parts[0][:2] in ["19", "20"]
                and len(range_parts[1]) == 2
            ):
                range_parts[1] = DateParser.fill_2digit_year(range_parts[1])
            # expand unambiguous 2-digit years to 4-digit years
            digit_parts_2 = [
                (idx, p)
                for idx, p in enumerate(range_parts)
                if p and re.match(r"\D*\s*\d{2}\s*\D*", p)
            ]
            digit_parts_4 = [
                p
                for p in range_parts
                if p and re.match(r"\D*\s*\d{4}\s*\D*", p)
            ]
            if len(digit_parts_2) == 1 and not digit_parts_4:
                yr = re.findall(r"\d{2}", digit_parts_2[0][1])[0]
                new_part = digit_parts_2[0][1].replace(
                    yr, DateParser.fill_2digit_year(yr)
                )
                range_parts[digit_parts_2[0][0]] = new_part
            # find global years in range in case some parts have to have year added
            # or adjusted
            global_years = [
                DateParser.extract_year(part)
                for part in range_parts
                if DateParser.extract_year(part)
            ]
            # find global seasons in range in case some parts have to have season month added
            global_seasons = [
                part
                for part in range_parts
                if any([p for p in seasonwords.keys() if p in part])
            ]
            for i, part in enumerate(range_parts):
                if id == print_id:
                    print(f"part: {part}")
                if not valid_date(part):
                    # handle dates like "winter/spring 2019"
                    if (
                        not DateParser.extract_year(part)
                        and len(global_years) > 0
                    ):
                        invalid, repaired = DateParser.repair_date(
                            part + " " + global_years[0], id
                        )
                    else:
                        invalid, repaired = DateParser.repair_date(part, id)
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
                # re-add month for end of season dates
                if len(global_seasons) == 1 and len(range_parts[1]) == 4:
                    seasonstart = [
                        v
                        for k, v in seasonwords.items()
                        if k in global_seasons[0]
                    ][0]
                    season_ends = {
                        "03": "05",
                        "06": "08",
                        "09": "11",
                        "12": "02",
                    }
                    range_parts[1] = (
                        f"{range_parts[1]}-{season_ends[seasonstart]}"
                    )
                # catch cases where ending is earlier than beginning
                if range_parts[0] > range_parts[1]:
                    # app.logger.debug(
                    #     f"invalid date range from {raw_range_parts}"
                    # )
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
                        # app.logger.debug(
                        #     f"adjusted winter date: {range_parts[0]}"
                        # )
                    else:
                        # app.logger.debug(
                        #     f"failed repairing invalid: {range_parts}"
                        # )
                        invalid = True
                # Check validity of first date part after we've adjusted
                # the year and month for season dates
                if not valid_date(range_parts[0]):
                    invalid = True
                date = "/".join(range_parts)
        return invalid, date
