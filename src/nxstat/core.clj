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

(def datetime-format "dd/MMM/yyyy:H:m:s")
(def datetimezone-format "dd/MMM/yyyy:H:m:s Z")

(defn date-formatter
  [format]
  (tf/formatter format))

(defn to-millisec
  "Returns the unix epoch milliseconds from date."
  ([^String date] (to-millisec date (date-formatter datetimezone-format)))
  ([^String date formatter]
   (try
     (tc/to-long (tf/parse formatter date))
     (catch Exception ex
       nil))))

(defn parse-datetime
  "Returns a formatted date or nil from a date string."
  ([^String date] (parse-datetime date (date-formatter datetimezone-format)))
  ([^String date fmtt]
   (try
     (tf/parse fmtt date)
     (catch Exception ex
       nil))))

(defn ymd
  [date]
  (first (string/split date #":")))

(defn ymdh
  [date-string]
  (if-let [[ymd h _] (string/split date-string #":")]
    (str ymd ":" h ":00:00")))

(defn hour
  [date-string]
  (if-let [[ymd h &rest] (string/split date-string #":")]
    h))

(defn same-day
  [^String date1 ^String date2]
  (= 0 (compare (ymd date1) (ymd date2))))

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

(defn visits-per-hour
  ([day-ds & [map-fn]]
   (let [map-fn (or map-fn hour)]
     (->> day-ds
          (map-ds :time_local :hour map-fn)
          (incanter/$group-by :hour)
          (map (fn [[k v]] (assoc k :visits (incanter/nrow v))))
          (sort-by :hour)
          incanter/to-dataset))))

(defn traffic
  [ds date metric & {:keys [as-time-series]}]
  (cond (= metric :day) (visits-per-hour (incanter/$where {:time_local {:fn #(same-day % date)}} ds)
                                         (and as-time-series #(to-millisec (ymdh %)))
                                         )
        (= metric :week) "traffic per week"
        (= metric :month) "traffic pet month"))

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

  "e.g: traffic for a given day as time-series-plot"
  (-> log-ds
      (traffic "08/Mar/2016:23:37:02 +0000" :day :as-time-series true)
      (incanter/with-data (icharts/time-series-plot :hour :visits))
      incanter/view
      )

  "e.g: traffic for a given day as bar chart"
  (-> log-ds
      (traffic "08/Mar/2016:23:37:02 +0000" :day)
      (incanter/with-data (icharts/bar-chart :hour :visits))
      incanter/view
      )

  )
