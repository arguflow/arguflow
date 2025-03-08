import { ChartState } from "@shopify/polaris-viz";
import { DateRangeFilter } from "app/components/analytics/DateRangePicker";
import {
  differenceInHours,
  eachDayOfInterval,
  format,
  isSameDay,
  subDays,
} from "date-fns";
import { SearchAnalyticsFilter } from "trieve-ts-sdk";

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
  const [datePart, timePart] = dateString.includes(" ")
    ? dateString.split(" ")
    : dateString.split("T");
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

export const formatStringDateRangeToDates = (
  range: SearchAnalyticsFilter["date_range"],
): DateRangeWithDates => {
  return {
    gt: range?.gt ? parseCustomDateString(range.gt) : undefined,
    lt: range?.lt ? parseCustomDateString(range.lt) : undefined,
    gte: range?.gte ? parseCustomDateString(range.gte) : undefined,
    lte: range?.lte ? parseCustomDateString(range.lte) : undefined,
  };
};

interface HasDateRange {
  date_range?: DateRangeWithDates | null;
}

export const transformAnalyticsFilter = (filter: HasDateRange) => {
  return {
    ...filter,
    date_range: filter.date_range
      ? transformDateParams(filter.date_range)
      : undefined,
  };
};

type DateRangeWithDates = {
  gt?: Date;
  lt?: Date;
  gte?: Date;
  lte?: Date;
};

export const transformDateParams = (params: DateRangeWithDates) => {
  return {
    gt: params.gt
      ? formatDateForApi(params.gt)
      : (undefined as string | null | undefined),
    lt: params.lt
      ? formatDateForApi(params.lt)
      : (undefined as string | null | undefined),
    gte: params.gte
      ? formatDateForApi(params.gte)
      : (undefined as string | null | undefined),
    lte: params.lte
      ? formatDateForApi(params.lte)
      : (undefined as string | null | undefined),
  };
};

export const formatSensibleTimestamp = (
  date: Date,
  range: DateRangeWithDates,
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

export const queryStateToChartState = (
  queryState: "error" | "success" | "pending",
): ChartState => {
  if (queryState === "error") {
    return "Error" as ChartState;
  }
  if (queryState === "success") {
    return "Success" as ChartState;
  }
  return "Loading" as ChartState;
};

export const fillDate = <T>({
  dataKey,
  timestampKey,
  data,
  date_range,
  defaultValue = 0,
}: {
  dataKey: keyof T;
  timestampKey: keyof T;
  data: T[];
  date_range: SearchAnalyticsFilter["date_range"] | undefined;
  defaultValue?: number | null;
}) => {
  console.log("date_range", date_range);
  const startDate = date_range?.gte || subDays(new Date(), 7);
  const endDate = date_range?.lte || new Date();

  const info = eachDayOfInterval({
    start: startDate,
    end: endDate,
  }).map((d) => {
    let foundDataPoint = null;

    for (const curr of data) {
      const parsedDate = new Date(
        parseCustomDateString(curr[timestampKey] as string),
      );
      if (isSameDay(parsedDate, d)) {
        foundDataPoint = {
          time: parsedDate,
          value: (curr[dataKey] as number) || 0, // Use || 0 to handle undefined/null
        };
        break; // Exit loop after finding a match
      }
    }

    return foundDataPoint
      ? foundDataPoint
      : {
          time: d,
          value: defaultValue,
        };
  });

  return info;
};
