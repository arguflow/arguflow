import { differenceInHours, format } from "date-fns";
import { AnalyticsFilter, DateRangeFilter } from "shared/types";

export const formatDateForApi = (date: Date) => {
  return date
    .toLocaleString("en-CA", {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
      timeZone: "UTC",
    })
    .replace(",", "");
};

export const parseCustomDateString = (dateString: string) => {
  const [datePart, timePart] = dateString.split(" ");
  /* eslint-disable prefer-const */
  let [year, month, day] = datePart.split("-");
  /* eslint-disable prefer-const */
  let [hour, minute, second] = timePart.split(":");
  let [wholeSec] = second.split(".");

  month = month.padStart(2, "0");
  day = day.padStart(2, "0");
  hour = hour.padStart(2, "0");
  minute = minute.padStart(2, "0");
  wholeSec = wholeSec.padStart(2, "0");

  const isoString = `${year}-${month}-${day}T${hour}:${minute}:${wholeSec}Z`;

  return new Date(isoString);
};

interface HasDateRange {
  date_range?: DateRangeFilter;
}

export const transformAnalyticsFilter = (filter: HasDateRange) => {
  return {
    ...filter,
    date_range: filter.date_range
      ? transformDateParams(filter.date_range)
      : undefined,
  };
};

export const transformDateParams = (params: DateRangeFilter) => {
  return {
    gt: params.gt ? formatDateForApi(params.gt) : undefined,
    lt: params.lt ? formatDateForApi(params.lt) : undefined,
    gte: params.gte ? formatDateForApi(params.gte) : undefined,
    lte: params.lte ? formatDateForApi(params.lte) : undefined,
  };
};

export const formatSensibleTimestamp = (
  date: Date,
  range: AnalyticsFilter["date_range"],
): string => {
  const highTime = range.lt || range.lte || new Date();
  if (!highTime) {
    return date.toLocaleString();
  }
  const lowTime = range.gt || range.gte;
  if (!lowTime) {
    return date.toLocaleDateString();
  }

  const hourDifference = differenceInHours(highTime, lowTime);
  // If the hour difference is 24 hours or less, format only with the time
  if (hourDifference <= 24) {
    return format(date, "HH:mm:ss");
  }

  // If the hour difference is 7 days or less, format with the date and time
  if (hourDifference <= 24 * 7) {
    return date.toLocaleDateString();
  }

  // If the hour difference is 30 days or less, format with the date
  if (hourDifference <= 24 * 30) {
    return date.toLocaleDateString();
  }

  return date.toLocaleDateString();
};

export function convertToISO8601(dateString: string) {
  // Split the input string into date, time, and timezone parts
  const [datePart, timePart] = dateString.split(" ");

  // Split the date part into year, month, and day
  const [year, month, day] = datePart.split("-");

  // Split the time part into hours, minutes, seconds, and milliseconds
  const [hours, minutes, secondsWithMs] = timePart.split(":");
  const [seconds, milliseconds] = secondsWithMs.split(".");

  // Construct the ISO 8601 string
  const isoString = `${year}-${month.padStart(2, "0")}-${day.padStart(
    2,
    "0",
  )}T${hours.padStart(2, "0")}:${minutes.padStart(2, "0")}:${seconds.padStart(
    2,
    "0",
  )}.${milliseconds.padEnd(3, "0")}Z`;

  return isoString;
}
