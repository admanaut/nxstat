(ns nxstat.core
  (:require [clojure.java.io :as io]
            [incanter.core :as incanter]
            [incanter.charts :as icharts]
            [clojure.string :as string]
            [clj-time.core :as t]
            [clj-time.format :as f])
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

(def date-format "dd/MMM/yyyy:h:m:s Z")
(def date-formatter (f/formatter date-format))

(defn parse-date
  ([date] (parse-date date date-formatter))
  ([^String date fmtt]
   (try
     (f/parse fmtt date)
     (catch Exception ex
       nil))))

(defn parse-line
  [^String line]
  (if-let [matches (re-find line-regex line)]
    (apply hash-map (interleave headers (next matches)))))


(def file-pattern #"access.log(.*)")
(def file-matches #(re-matches file-pattern %))
(def asc compare)
(def desc #(compare %2 %1))

(defn ls-dir
  [^String dir ffilter fsort]
  (sort fsort (filter ffilter (.list (io/file dir) ))))

(defn lazy-read-lines
  [file]
  (let [in-file (io/reader file)
        line-seq (line-seq in-file)
        lazy (fn lazy [wrapped]
               (lazy-seq
                (if-let [s (seq wrapped)]
                  (cons (first s) (lazy (rest s)))
                  (.close in-file))))]
    (lazy line-seq)))

(defn lazy-read-files
  [files]
  (let [lazy-lines (fn lazy-lines [lines-seq fs]
                     (lazy-seq
                      (cond (seq lines-seq) (cons (first lines-seq) (lazy-lines (rest lines-seq) fs))
                            (seq fs) (let [l (lazy-read-lines (first fs))]
                                       (cons (first l) (lazy-lines (rest l) (rest fs))))
                            :else nil)))]
    (when-let [fx (seq files)]
      (lazy-lines (lazy-read-lines (first fx)) (rest fx)))))

(defn load-log-file
  [file]
  (->>
   (map parse-line )
   (incanter/dataset headers)))

(defn load-logs
  [log-files]
  (->> log-files
       lazy-read-files
       (map parse-line)
       (incanter/dataset headers)))




(comment

  (defn lines-seq
    [files]
    (when-let [f (first files)]
      (cons (line-seq (io/reader f)) (lazy-seq (lines-seq (rest files))))))

  (defn load-logs
    [log-files]
    (->> (map )
         ))

  #(str dir %) (ls-dir dir)

  (defn -main
    "I don't do a whole lot ... yet."
    [& args]
    (println "Hello, World!")))
