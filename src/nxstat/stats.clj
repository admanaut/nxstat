(ns nxstat.stats
  (:require [nxstat.datetime :as nxdt]
            [incanter.core :as incanter]))

(defn map-ds
  [in-col out-col f ds]
  (incanter/dataset [out-col] (remove nil? (incanter/$map f in-col ds))))

;; = Traffic =

(defn visits
  [ds grp-by]
  (->> ds
       (incanter/$group-by grp-by)
       (map (fn [[k v]] (assoc k :visits (incanter/nrow v))))
       (sort-by grp-by)
       incanter/to-dataset))

(defn visits-per-hour
  [ds]
  (visits (map-ds :time_local :hour #(nxdt/to-millisec (nxdt/translate-date % :hour)) ds) :hour))

(defn visits-per-day
  [ds]
  (visits (map-ds :time_local :day #(nxdt/to-millisec (nxdt/translate-date % :day)) ds) :day))

(defn visits-per-month
  [ds]
  (visits (map-ds :time_local :month #(nxdt/to-millisec (nxdt/translate-date % :month)) ds) :month))

(defn traffic
  [ds date metric]
  (cond (= metric :day)   (visits-per-hour (incanter/$where {:time_local {:fn #(nxdt/same-day? % date)}} ds))
        (= metric :week)  (visits-per-day  (incanter/$where {:time_local {:fn #(nxdt/same-week? % date)}} ds))
        (= metric :month) (visits-per-day  (incanter/$where {:time_local {:fn #(nxdt/same-month? % date)}} ds))
        (= metric :year)  (visits-per-month (incanter/$where {:time_local {:fn #(nxdt/same-year? % date)}} ds))))

(defn overview
  [ds interval granularity]
  (let [[from to] interval
        interval-ds (incanter/$where {:time_local {:fn #((partial nxdt/within? from (or to (nxdt/now))) %)}} ds)]
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
