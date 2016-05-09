(ns nxstat.core
  (:require [clojure.java.io :as io]
            [incanter.core :as incanter]
            [incanter.charts :as icharts]
            [clojure.string :as string]
            [clj-time.core :as tt]
            [clj-time.format :as tf]
            [clj-time.coerce :as tc])
  (:gen-class))

(def headers [:remote_addr
              :time_local
              :request_method
              :request_path
              :status
              :body_bytes_sent
              :http_referer
              :http_user_agent
              :http_x_forwarded_for
              :request_time
              :connection])

(def line-regex  #"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})? - - \[(.*)\] \"(\w+) ([^\"]*)\"(\d{3}) (\d+) \"([^\"]*)\" \"([^\"]*)\" \"([^\"]*)\"\[(.*)\] \[(.*)\]")

(def datetime-format "dd/MMM/yyyy:H:m:s Z")

(defn date-formatter
  [format]
  (tf/formatter format))

(defn to-millisec
  "Returns the unix epoch milliseconds from date."
  ([^String date] (to-millisec date (date-formatter datetime-format)))
  ([^String date formatter]
   (try
     (tc/to-long (tf/parse formatter date))
     (catch Exception ex
       nil))))

(defn parse-date
  [^String date formatter]
  (try
    (tf/parse formatter date)
    (catch Exception ex
      nil)))

(defn explode-date
  [date-string]
  (let [parsed (parse-date date-string (date-formatter datetime-format) )]
    {:y (.get (.year parsed))
     :m (.get (.monthOfYear parsed))
     :w (.get (.weekOfWeekyear parsed))
     :d (.get (.dayOfMonth parsed))
     :h (.get (.hourOfDay parsed))
     :i (.get (.minuteOfHour parsed))
     :s (.get (.secondOfMinute parsed))}))


(defn translate-date
  "Formats a given date to the specified granularity.

  gr - [keyword] one of :year :month :day :minute :hour :second"
  [date-str gr]
  (let [{:keys [y m d h i s]} (explode-date date-str)
        formatter (date-formatter datetime-format)]
    (cond (= gr :year)   (tf/unparse formatter (tt/date-midnight y))
          (= gr :month)  (tf/unparse formatter (tt/date-midnight y m))
          (= gr :day)    (tf/unparse formatter (tt/date-time y m d))
          (= gr :hour)   (tf/unparse formatter (tt/date-time y m d h))
          (= gr :minute) (tf/unparse formatter (tt/date-time y m d h i))
          (= gr :second) (tf/unparse formatter (tt/date-time y m d h i s)))))

(defn dates-cmp-key
  [^String date1 ^String date2 & fields]
  (let [p1 (explode-date date1)
        p2 (explode-date date2)]
    (loop [fs fields
           rez true]
      (if (seq fs)
        (recur (rest fs) (and rez (= (get p1 (first fs)) (get p2 (first fs)))))
        rez))))

(defn same-day?
  [^String date1 ^String date2]
  (dates-cmp-key date1 date2 :y :m :d))

(defn same-week?
  [^String date1 ^String date2]
  (dates-cmp-key date1 date2 :y :m :w))

(defn same-month?
  [^String date1 ^String date2]
  (dates-cmp-key date1 date2 :y :m))

(defn same-year?
  [^String date1 ^String date2]
  (dates-cmp-key date1 date2 :y))

(defn within?
  [^String from ^String to ^String within]
  (let [from-date (parse-date from (date-formatter datetime-format))
        to-date (parse-date to (date-formatter datetime-format))
        within-date (parse-date within (date-formatter datetime-format) )]
    (tt/within? (tt/interval from-date to-date)
                within-date)))

(defn now
  []
  (tf/unparse (date-formatter datetime-format)
              (tt/now)))

(defn parse-line
  "Returns a map where keys are line-keys and values are the result of
  re-find by line-re in line. Returns nil if no matches found."
  ([^String line] (parse-line line line-regex headers))
  ([^String line ^String line-re line-keys]
   (if-let [matches (re-find line-re line)]
     (apply hash-map (interleave line-keys (next matches))))))


(def file-pattern #"access.log(.*)")
(def file-matches #(re-matches file-pattern %))
(def asc compare)
(def desc #(compare %2 %1))

(defn ls-dir
  "List of files in dir filtered by ffilter and sorted by fsort.

  dir       - [String]   directory to list files from
  ffilter   - [fn]       fn used to filter filenames
  fsort     - [fn]       fn used by sort"
  [^String dir ffilter fsort]
  (sort fsort (filter ffilter (.list (io/file dir) ))))

(defn lazy-read-lines
  "Returns the lines of text from file as a sequence of strings.
   This function is not sutable for cases when the sequence is
   only partially consumed as the file handle won't be closed."
  [^String file]
  (let [in-file (io/reader file)
        line-seq (line-seq in-file)
        lazy (fn lazy [wrapped]
               (lazy-seq
                (if-let [s (seq wrapped)]
                  (cons (first s) (lazy (rest s)))
                  (.close in-file))))]
    (lazy line-seq)))

(defn lazy-read-files
  "Returns the lines of text from each file as a sequence of strings.
   This function is not sutable for cases when the sequence is
   only partially consumed as the file handle won't be closed."
  [files]
  (let [lazy-files (fn lazy-files [lines-seq fs]
                     (lazy-seq
                      (cond (seq lines-seq) (cons (first lines-seq) (lazy-files (rest lines-seq) fs))
                            (seq fs) (let [l (lazy-read-lines (first fs))]
                                       (cons (first l) (lazy-files (rest l) (rest fs))))
                            :else nil)))]
    (when-let [fx (seq files)]
      (lazy-files (lazy-read-lines (first fx)) (rest fx)))))


(defn load-logs
  "Reads, parses and formats each line of text from each log-file
  and returns a loaded incanter dataset."
  [log-files]
  (->> log-files
       lazy-read-files
       (map parse-line)
       (remove nil?)
       (incanter/to-dataset)))

(defn load-from-dir
  [dir]
  (load-logs (map #(str dir %) (ls-dir dir file-matches desc))))


;;  === Incanter datasets ===

(defn map-ds
  [in-col out-col f ds]
  (incanter/dataset [out-col] (remove nil? (incanter/$map f in-col ds))))

;;       = Traffic =
;; 1. traffic per [day week month]
;; 2. overlay traffic
;; 3. overview with granularity [day week month]

(defn visits
  [ds grp-by]
  (->> ds
       (incanter/$group-by grp-by)
       (map (fn [[k v]] (assoc k :visits (incanter/nrow v))))
       (sort-by grp-by)
       incanter/to-dataset))

(defn visits-per-hour
  [ds]
  (visits (map-ds :time_local :hour #(to-millisec (translate-date % :hour)) ds) :hour))

(defn visits-per-day
  [ds]
  (visits (map-ds :time_local :day #(to-millisec (translate-date % :day)) ds) :day))

(defn visits-per-month
  [ds]
  (visits (map-ds :time_local :month #(to-millisec (translate-date % :month)) ds) :month))

(defn traffic
  [ds date metric]
  (cond (= metric :day)   (visits-per-hour (incanter/$where {:time_local {:fn #(same-day? % date)}} ds))
        (= metric :week)  (visits-per-day  (incanter/$where {:time_local {:fn #(same-week? % date)}} ds))
        (= metric :month) (visits-per-day  (incanter/$where {:time_local {:fn #(same-month? % date)}} ds))
        (= metric :year)  (visits-per-month (incanter/$where {:time_local {:fn #(same-year? % date)}} ds))))

(defn overview
  [ds interval granularity]
  (let [[from to] interval
        interval-ds (incanter/$where {:time_local {:fn #((partial within? from (or to (now))) %)}} ds)]
    (cond (= granularity :hour)  (visits-per-hour interval-ds)
          (= granularity :day)   (visits-per-day  interval-ds)
          (= granularity :week)  (visits-per-day  interval-ds)
          (= granularity :month) (visits-per-month  interval-ds))))


;;      = Pages =
;; 1. popular pages

;;      = Referrals =
;; 1. top referrals

;;     = Technical =
;; 1. devices
;; 2. response code
;; 3. response time


(comment
  (def log-ds (load-from-dir "nginx/"))

  "=== Traffic ==="

  "e.g: traffic for a day as time-series-plot"
  (-> log-ds
      (traffic "08/Mar/2016:23:37:02 +0000" :day)
      (incanter/with-data (icharts/time-series-plot :hour :visits))
      incanter/view)

  "e.g: traffic for a day as bar chart"
  (-> log-ds
      (traffic "08/Mar/2016:23:37:02 +0000" :day)
      (incanter/with-data (icharts/bar-chart :hour :visits))
      incanter/view)

  "e.g: traffic for a week as time-series-plot"
  (-> log-ds
      (traffic "08/Mar/2016:23:37:02 +0000" :week)
      (incanter/with-data (icharts/time-series-plot :day :visits))
      incanter/view)

  "e.g: traffic for a week as bar chart"
  (-> log-ds
      (traffic "08/Apr/2016:23:37:02 +0000" :week)
      (incanter/with-data (icharts/bar-chart :day :visits))
      incanter/view)

  "e.g: traffic for a month as time-series-plot"
  (-> log-ds
      (traffic "18/Apr/2016:23:37:02 +0000" :month)
      (incanter/with-data (icharts/time-series-plot :day :visits))
      incanter/view)

  "e.g: traffic for a year as time-series-plot"
  (-> log-ds
      (traffic "18/Apr/2016:23:37:02 +0000" :year)
      (incanter/with-data (icharts/time-series-plot :month :visits))
      incanter/view)

  "=== Overview ==="

  "e.g: traffic overview, granularity hour"
  (-> log-ds
      (overview ["18/Mar/2016:23:37:02 +0000"] :hour)
      (incanter/with-data (icharts/time-series-plot :hour :visits))
      incanter/view)

  "e.g: traffic overview, granularity day"
  (-> log-ds
      (overview ["18/Apr/2016:23:37:02 +0000"] :day)
      (incanter/with-data (icharts/time-series-plot :day :visits))
      incanter/view)

  "e.g: traffic overview, granularity week"
  (-> log-ds
      (overview ["18/Apr/2016:23:37:02 +0000"] :week)
      (incanter/with-data (icharts/time-series-plot :day :visits))
      incanter/view
      )

  )
