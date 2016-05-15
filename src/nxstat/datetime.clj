(ns nxstat.datetime
  (:require [clj-time.core :as tt]
            [clj-time.format :as tf]
            [clj-time.coerce :as tc]))

(def formats {:date-time "dd/MMM/yyyy:H:m:s Z"
              :hour-minute "H:m"})

(def datetime-format "dd/MMM/yyyy:H:m:s Z")

(defn formatter
  [format]
  (tf/formatter format))

(defn to-millisec
  "Returns the unix milliseconds from date."
  ([^String date] (to-millisec date (formatter datetime-format)))
  ([^String date formatter]
   (try
     (tc/to-long (tf/parse formatter date))
     (catch Exception ex
       nil))))

(defn parse-date
  "Returns a org.joda.time.DateTime object from date formatted by formatter, or
  nil when format is invalid."
  [^String date formatter]
  (try
    (tf/parse formatter date)
    (catch Exception ex
      nil)))

(defn explode-date
  "Returns a map of date's constituent parts."
  [date]
  (let [parsed (parse-date date (formatter datetime-format) )]
    {:y (.get (.year parsed))
     :m (.get (.monthOfYear parsed))
     :w (.get (.weekOfWeekyear parsed))
     :d (.get (.dayOfMonth parsed))
     :h (.get (.hourOfDay parsed))
     :i (.get (.minuteOfHour parsed))
     :s (.get (.secondOfMinute parsed))}))

(defn from-millisec
  ([^Long timestamp] (from-millisec timestamp :date-time))
  ([^Long timestamp frmt]
   (when-let [f (get formats frmt)]
     (let [date (tc/from-long timestamp)]
       (try
         (tf/unparse (formatter f) date)
         (catch Exception ex
           nil))))))

(defn translate-date
  "Returns a new date where parts are copied up to the given granularity and
  the rest of the parts being filled in with their identity.

  gr - [keyword] one of :year :month :day :minute :hour :second

  e.g: given date 20/Mar/2016:09:12:26 +0000 and gr :hour,
       returns  20/Mar/2016:09:0:0 +0000
       where year,month,day and hour are copied and minutes and seconds are filled in with 0.
  "
  [date gr]
  (let [{:keys [y m d h i s]} (explode-date date)
        formatter (formatter datetime-format)]
    (cond (= gr :year)   (tf/unparse formatter (tt/date-midnight y))
          (= gr :month)  (tf/unparse formatter (tt/date-midnight y m))
          (= gr :day)    (tf/unparse formatter (tt/date-time y m d))
          (= gr :hour)   (tf/unparse formatter (tt/date-time y m d h))
          (= gr :minute) (tf/unparse formatter (tt/date-time y m d h i))
          (= gr :second) (tf/unparse formatter (tt/date-time y m d h i s)))))

(defn dates-cmp-key
  "Returns the result of comparing dates by each key."
  [^String date1 ^String date2 & fields]
  (let [p1 (explode-date date1)
        p2 (explode-date date2)]
    (when-let [fseq (seq fields)]
      (loop [fs fseq
             rez true]
        (if-let [[field & rfields] (seq fs)]
            (recur rfields (and rez (= (field p1) (field p2))))
          rez)))))

(defn same-day?
  "Returns true if dates are the same day of the month of the year, false otherwise."
  [^String date1 ^String date2]
  (dates-cmp-key date1 date2 :y :m :d))

(defn same-week?
  "Returns true if date are part of the same week of the year, false otherwise."
  [^String date1 ^String date2]
  (dates-cmp-key date1 date2 :y :m :w))

(defn same-month?
  "Returns true if dates are parth of the same month of the year, false otherwise."
  [^String date1 ^String date2]
  (dates-cmp-key date1 date2 :y :m))

(defn same-year?
  "Returns true if dates share the same year, false otherwise."
  [^String date1 ^String date2]
  (dates-cmp-key date1 date2 :y))

(defn within?
  "Returns true if test is within from and to, false otherwise."
  [^String from ^String to ^String test]
  (let [from-date (parse-date from (formatter datetime-format))
        to-date (parse-date to (formatter datetime-format))
        test-date (parse-date test (formatter datetime-format) )]
    (tt/within? (tt/interval from-date to-date)
                test-date)))

(defn now
  []
  "Returns today's date."
  (tf/unparse (formatter datetime-format) (tt/now)))

(defn beginning
  []
  "Returns the datetime of the beginning of the unix epoch."
  (tf/unparse (formatter datetime-format) (tt/date-time 1970 1 1)))
