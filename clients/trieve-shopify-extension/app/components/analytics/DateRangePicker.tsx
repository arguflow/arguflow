import {
  BlockStack,
  Box,
  Button,
  DatePicker,
  InlineGrid,
  InlineStack,
  OptionList,
  Popover,
  Scrollable,
  Select,
  TextField,
  useBreakpoints,
} from "@shopify/polaris";
import { useEffect, useRef, useState } from "react";
import { subDays, subHours, subMinutes, subMonths } from "date-fns";
import { Granularity } from "trieve-ts-sdk";

// Types from the original code
export type DateRangeFilter = {
  gt?: Date | undefined;
  gte?: Date | undefined;
  lt?: Date | undefined;
  lte?: Date | undefined;
};

type DatePeriod = {
  since: Date | undefined;
  until: Date | undefined;
};

interface DatePreset {
  id?: number;
  title: string;
  alias: string;
  period: DatePeriod;
  granularity: Granularity;
}

interface DateRangePickerProps {
  value: DateRangeFilter;
  onChange: (value: DateRangeFilter) => void;
  onGranularitySuggestion?: (granularity: Granularity) => void;
  presets?: DatePreset[];
  label?: string;
}

const defaultDatePresets: DatePreset[] = [
  {
    id: 1,
    title: "Last 15 Minutes",
    alias: "last15min",
    period: {
      since: subMinutes(new Date(), 15),
      until: new Date(),
    },
    granularity: "minute",
  },
  {
    id: 2,
    title: "Last 30 Minutes",
    alias: "last30min",
    period: {
      since: subMinutes(new Date(), 30),
      until: new Date(),
    },
    granularity: "minute",
  },
  {
    id: 3,
    title: "Last Hour",
    alias: "lastHour",
    period: {
      since: subHours(new Date(), 1),
      until: new Date(),
    },
    granularity: "minute",
  },
  {
    id: 4,
    title: "Last 3 Hours",
    alias: "last3Hours",
    period: {
      since: subHours(new Date(), 3),
      until: new Date(),
    },
    granularity: "hour",
  },
  {
    id: 5,
    title: "Last 12 Hours",
    alias: "last12Hours",
    period: {
      since: subHours(new Date(), 12),
      until: new Date(),
    },
    granularity: "hour",
  },
  {
    id: 6,
    title: "Last 24 Hours",
    alias: "last24Hours",
    period: {
      since: subHours(new Date(), 24),
      until: new Date(),
    },
    granularity: "hour",
  },
  {
    id: 7,
    title: "Last 7 Days",
    alias: "last7days",
    period: {
      since: subDays(new Date(), 7),
      until: new Date(),
    },
    granularity: "day",
  },
  {
    id: 8,
    title: "Last Month",
    alias: "lastMonth",
    period: {
      since: subMonths(new Date(), 1),
      until: new Date(),
    },
    granularity: "day",
  },
  {
    id: 9,
    title: "All Time",
    alias: "allTime",
    period: {
      since: undefined,
      until: new Date(),
    },
    granularity: "month",
  },
];

// Convert DateRangeFilter to a range period
const convertDateRangeToRangePeriod = (
  dateRange: DateRangeFilter | undefined,
): DatePeriod => {
  const since = dateRange?.gte || dateRange?.gt;
  const until = dateRange?.lte || dateRange?.lt || new Date();

  return {
    since,
    until,
  };
};

// Find a matching preset from the date range
const findMatchingPreset = (
  dateRange: DateRangeFilter | undefined,
  presets: DatePreset[],
): DatePreset => {
  if (!dateRange) return presets[0];

  const { since, until } = convertDateRangeToRangePeriod(dateRange);

  // For "now" ranges, match based on the "since" value
  if (
    !until ||
    Math.abs((until.getTime() - new Date().getTime()) / 1000) < 60
  ) {
    for (const preset of presets) {
      if (preset.period.since && since) {
        // Allow 2-second tolerance for time-based comparisons
        const diff = Math.abs(
          (preset.period.since.getTime() - since.getTime()) / 1000,
        );
        if (diff < 2) {
          return preset;
        }
      }
    }
  }

  // Try exact match
  const matchingPreset = presets.find((preset) => {
    if (!preset.period.since && !since) return true;
    if (!preset.period.since || !since) return false;

    const sinceMatch =
      Math.abs((preset.period.since.getTime() - since.getTime()) / 1000) < 2;
    const untilMatch =
      until && preset.period.until
        ? Math.abs((preset.period.until.getTime() - until.getTime()) / 1000) < 2
        : false;

    return sinceMatch && untilMatch;
  });

  if (matchingPreset) return matchingPreset;

  // Return custom preset
  return {
    title: "Custom",
    alias: "custom",
    period: { since, until },
    granularity: "hour", // Default granularity for custom ranges
  };
};

const getRangeLabel = (activeDateRange: DatePreset): string => {
  if (activeDateRange.title !== "Custom") {
    return activeDateRange.title;
  }

  if (!activeDateRange.period.since) {
    return "All Time";
  }

  return `${activeDateRange.period.since.toLocaleDateString()} - ${
    activeDateRange.period.until?.toLocaleDateString() || "Now"
  }`;
};

interface DateRef {
  year: number;
  month: number;
}

interface InputValues {
  since: string;
  until: string;
}

interface CalendarChangeData {
  start: Date;
  end: Date;
}

export function DateRangePicker({
  value,
  onChange,
  onGranularitySuggestion,
  presets = defaultDatePresets,
  label = "Date Range",
}: DateRangePickerProps): JSX.Element {
  const { mdDown, lgUp } = useBreakpoints();
  const shouldShowMultiMonth = lgUp;

  // State
  const [popoverActive, setPopoverActive] = useState<boolean>(false);
  const [activeDateRange, setActiveDateRange] = useState<DatePreset>(() =>
    findMatchingPreset(value, presets),
  );

  const [inputValues, setInputValues] = useState<InputValues>({
    since: activeDateRange.period.since
      ? formatDateToYearMonthDayDateString(activeDateRange.period.since)
      : "",
    until: activeDateRange.period.until
      ? formatDateToYearMonthDayDateString(activeDateRange.period.until)
      : "",
  });

  const [{ month, year }, setDate] = useState<DateRef>(() => {
    const referenceDate = activeDateRange.period.until || new Date();
    return {
      month: referenceDate.getMonth(),
      year: referenceDate.getFullYear(),
    };
  });

  const datePickerRef = useRef<HTMLDivElement | null>(null);

  // Date formatting helpers
  const VALID_YYYY_MM_DD_DATE_REGEX = /^\d{4}-\d{1,2}-\d{1,2}/;

  function isDate(date: string): boolean {
    return !isNaN(new Date(date).getDate());
  }

  function isValidYearMonthDayDateString(date: string): boolean {
    return VALID_YYYY_MM_DD_DATE_REGEX.test(date) && isDate(date);
  }

  function isValidDate(date: string): boolean {
    return date.length === 10 && isValidYearMonthDayDateString(date);
  }

  function parseYearMonthDayDateString(input: string): Date {
    const [year, month, day] = input.split("-");
    return new Date(Number(year), Number(month) - 1, Number(day));
  }

  function formatDateToYearMonthDayDateString(date: Date | undefined): string {
    if (!date) return "";

    const year = String(date.getFullYear());
    let month = String(date.getMonth() + 1);
    let day = String(date.getDate());

    if (month.length < 2) {
      month = String(month).padStart(2, "0");
    }
    if (day.length < 2) {
      day = String(day).padStart(2, "0");
    }

    return [year, month, day].join("-");
  }

  // Handlers
  function handleStartInputValueChange(value: string): void {
    setInputValues((prevState) => {
      return { ...prevState, since: value };
    });

    if (isValidDate(value)) {
      const newSince = parseYearMonthDayDateString(value);
      setActiveDateRange((prevState) => {
        const newPeriod: DatePeriod =
          prevState.period &&
          prevState.period.until &&
          newSince <= prevState.period.until
            ? { since: newSince, until: prevState.period.until }
            : { since: newSince, until: newSince };

        return {
          ...prevState,
          title: "Custom",
          alias: "custom",
          period: newPeriod,
        };
      });
    }
  }

  function handleEndInputValueChange(value: string): void {
    setInputValues((prevState) => ({ ...prevState, until: value }));

    if (isValidDate(value)) {
      const newUntil = parseYearMonthDayDateString(value);
      setActiveDateRange((prevState) => {
        const newPeriod: DatePeriod =
          prevState.period &&
          prevState.period.since &&
          newUntil >= prevState.period.since
            ? { since: prevState.period.since, until: newUntil }
            : { since: newUntil, until: newUntil };

        return {
          ...prevState,
          title: "Custom",
          alias: "custom",
          period: newPeriod,
        };
      });
    }
  }

  function handleMonthChange(month: number, year: number): void {
    setDate({ month, year });
  }

  function handleCalendarChange({ start, end }: CalendarChangeData): void {
    const newDateRange = presets.find((range) => {
      return (
        range.period.since?.valueOf() === start.valueOf() &&
        range.period.until?.valueOf() === end.valueOf()
      );
    }) || {
      alias: "custom",
      title: "Custom",
      period: {
        since: start,
        until: end,
      },
      granularity: "hour" as Granularity,
    };

    setActiveDateRange(newDateRange);
  }

  function apply(): void {
    // Convert the selected date range to the format expected by the API
    const { since, until } = activeDateRange.period;
    const dateRange: DateRangeFilter = {
      gte: since,
      lte: until,
      gt: undefined,
      lt: undefined,
    };

    // Call onChange with the formatted date range
    onChange(dateRange);

    // Suggest granularity if handler is provided
    if (onGranularitySuggestion) {
      onGranularitySuggestion(activeDateRange.granularity);
    }

    setPopoverActive(false); // Close the Popover
  }

  function cancel(): void {
    // Reset to the current value
    setActiveDateRange(findMatchingPreset(value, presets));
    setPopoverActive(false); // Close the Popover
  }

  const handlePresetSelect = (preset: DatePreset): void => {
    setActiveDateRange(preset);
    const dateRange: DateRangeFilter = {
      gte: preset.period.since,
      lte: preset.period.until,
      gt: undefined,
      lt: undefined,
    };
    onChange(dateRange);
    if (onGranularitySuggestion) {
      onGranularitySuggestion(preset.granularity);
    }
    setPopoverActive(false); // Close the Popover
  };

  // When external value changes, update internal state
  useEffect(() => {
    const newActiveDateRange = findMatchingPreset(value, presets);
    setActiveDateRange(newActiveDateRange);
  }, [value, presets]);

  // When active date range changes, update input values
  useEffect(() => {
    if (activeDateRange) {
      setInputValues({
        since: activeDateRange.period.since
          ? formatDateToYearMonthDayDateString(activeDateRange.period.since)
          : "",
        until: activeDateRange.period.until
          ? formatDateToYearMonthDayDateString(activeDateRange.period.until)
          : "",
      });

      // Update month/year if needed
      function monthDiff(referenceDate: DateRef, newDate: DateRef): number {
        return (
          newDate.month -
          referenceDate.month +
          12 * (referenceDate.year - newDate.year)
        );
      }

      if (activeDateRange.period.until) {
        const monthDifference = monthDiff(
          { year, month },
          {
            year: activeDateRange.period.until.getFullYear(),
            month: activeDateRange.period.until.getMonth(),
          },
        );

        if (monthDifference > 1 || monthDifference < 0) {
          setDate({
            month: activeDateRange.period.until.getMonth(),
            year: activeDateRange.period.until.getFullYear(),
          });
        }
      }
    }
  }, [activeDateRange, month, year]);

  const buttonLabel = getRangeLabel(activeDateRange);

  return (
    <div>
      <Box>
        <div className="text-sm text-neutral-600">{label}</div>
      </Box>
      <Popover
        active={popoverActive}
        autofocusTarget="none"
        preferredAlignment="left"
        preferredPosition="below"
        fluidContent
        sectioned={false}
        fullHeight
        activator={
          <Button
            size="slim"
            onClick={() => setPopoverActive(!popoverActive)}
            fullWidth
          >
            {buttonLabel}
          </Button>
        }
        onClose={() => setPopoverActive(false)}
      >
        <Popover.Pane fixed>
          <InlineGrid
            columns={{
              xs: "1fr",
              md: "max-content max-content",
            }}
            gap={"600"}
            // @ts-ignore - Ref type mismatch in Polaris types
            ref={datePickerRef}
          >
            <Box
              maxWidth={mdDown ? "516px" : "212px"}
              width={mdDown ? "100%" : "212px"}
              padding={{ xs: "500", md: "0" }}
              paddingBlockEnd={{ xs: "100", md: "0" }}
            >
              {mdDown ? (
                <Select
                  label="Date range"
                  labelHidden
                  onChange={(value: string) => {
                    const result = presets.find(
                      ({ title, alias }) => title === value || alias === value,
                    );
                    if (result) {
                      setActiveDateRange(result);
                    }
                  }}
                  value={activeDateRange?.title || activeDateRange?.alias || ""}
                  options={presets.map(({ alias, title }) => title || alias)}
                />
              ) : (
                <Scrollable style={{ height: "334px" }}>
                  <OptionList
                    options={presets.map((range) => ({
                      value: range.alias,
                      label: range.title,
                    }))}
                    selected={[activeDateRange.alias]}
                    onChange={(value: string[]) => {
                      const result = presets.find(
                        (range) => range.alias === value[0],
                      );
                      if (result) {
                        setActiveDateRange(result);
                      }
                    }}
                  />
                </Scrollable>
              )}
            </Box>
            <Box padding={{ xs: "500" }} maxWidth={mdDown ? "320px" : "516px"}>
              <BlockStack gap="400">
                <InlineStack gap="200">
                  <div style={{ flexGrow: 1 }}>
                    <TextField
                      role="combobox"
                      label="Since"
                      value={inputValues.since}
                      onChange={handleStartInputValueChange}
                      autoComplete="off"
                    />
                  </div>
                  <div style={{ flexGrow: 1 }}>
                    <TextField
                      role="combobox"
                      label="Until"
                      value={inputValues.until}
                      onChange={handleEndInputValueChange}
                      autoComplete="off"
                    />
                  </div>
                </InlineStack>
                <div>
                  <DatePicker
                    month={month}
                    year={year}
                    selected={{
                      start: activeDateRange.period.since || new Date(0),
                      end: activeDateRange.period.until || new Date(),
                    }}
                    onMonthChange={handleMonthChange}
                    onChange={handleCalendarChange}
                    multiMonth={shouldShowMultiMonth}
                    allowRange
                  />
                </div>
              </BlockStack>
            </Box>
          </InlineGrid>
        </Popover.Pane>
        <Popover.Pane fixed>
          <Popover.Section>
            <InlineStack align="end">
              <Button onClick={cancel}>Cancel</Button>
              <Button onClick={apply}>Apply</Button>
            </InlineStack>
          </Popover.Section>
        </Popover.Pane>
      </Popover>
    </div>
  );
}
