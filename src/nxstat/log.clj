(ns nxstat.log
  (:require [clojure.java.io :as io]
            [incanter.core :as incanter]))

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

(def access-log-pattern #"access.log(.*)")
(def access-log-matcher #(re-matches access-log-pattern %))
(def asc compare)
(def desc #(compare %2 %1))

(defn parse-line
  "Returns a map where keys are line-keys and values are the result of
  re-find by line-re in line. Returns nil if no matches found."
  ([^String line] (parse-line line line-regex headers))
  ([^String line ^String line-re line-keys]
   (if-let [matches (re-find line-re line)]
     (apply hash-map (interleave line-keys (next matches))))))

(defn ls-dir
  "Returns a list of files found at dir filtered by ffilter and sorted by fsort,
   nil when dir is not a directory.

  dir       - [String]   directory to list files from
  ffilter   - [fn]       fn used to filter filenames
  fsort     - [fn]       fn used by sort"
  [^String dir ffilter fsort]
  (when (.isDirectory (io/file dir))
    (sort fsort (filter ffilter (.list (io/file dir) )))))

(defn- add-slash
  [^String path]
  (str path (when-not (.endsWith path "/") "/")))

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


(defn load-files
  "Reads, parses and formats each line of text from each log-file
  and returns a loaded incanter dataset."
  [log-files]
  (->> log-files
       lazy-read-files
       (map parse-line)
       (remove nil?)
       (incanter/to-dataset)))

(defn load
  ""
  ([location] (load location access-log-matcher desc))
  ([location file-matcher sorter]
   (let [file (io/file location)]
     (cond (.isDirectory file) (load-files (map #(str (add-slash location) %) (ls-dir file file-matcher sorter)))
           (.isFile file)      (load-files [file])))))
