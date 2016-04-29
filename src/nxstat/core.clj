(ns nxstat.core
  (:require [clojure.java.io :as io]
            [incanter.core :as incanter]
            [clojure.string :as string])
  (:gen-class))


(def headers [:GEOID :SUMLEV :STATE :COUNTY :CBSA :CSA :NECTA :CNECTA :NAME :POP100 :HU100 :POP100.2000 :HU100.2000 :P035001 :P035001.2000
])

(defn ls
  [^String dir]
  (lazy-seq (sort (.list (io/file dir)))))

(defn split-line
  [^String line]
  (string/split line #","))

(defn lines-seq
  [files]
  (when-let [f (first files)]
    (cons (line-seq (io/reader f)) (lazy-seq (lines-seq (next files))))))


(defn load-logs
  [dir]
  (lines-seq (map #(str dir %) (ls dir)) ))

;(incanter/dataset headers lines)

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
