(ns nxstat.stats
  (:require [nxstat.datetime :as nxdt]
            [com.rpl.specter :as specter]))

(defn map-ds
  [ds f col]
  (specter/transform [specter/ALL col] f ds))

(defn sel-ds
  [ds & cols]
  (map #(select-keys % cols) ds))

(defn filter-ds-interval
  [ds from to]
  (specter/select [specter/ALL #((partial nxdt/within? from to) %)] ds))

;; = Traffic =

(defn visits
  [ds col]
  (->> ds
       (group-by col)
       (map (fn [[k v]] {col k :count (count v)}))
       (sort-by col)))

(defn visits-per-hour
  [ds]
  (visits
   (-> ds
       (map-ds #(nxdt/to-millisec (nxdt/translate-date % :hour)) :time_local)
       (sel-ds :time_local))
   :time_local))

(defn visits-per-day
  [ds]
  (visits
   (-> ds
       (map-ds #(nxdt/to-millisec (nxdt/translate-date % :day)) :time_local)
       (sel-ds :time_local))
   :time_local))

(defn visits-per-month
  [ds]
  (visits
   (-> ds
       (map-ds #(nxdt/to-millisec (nxdt/translate-date % :month)) :time_local)
       (sel-ds :time_local))
   :time_local))

(defn traffic
  [ds date metric]
  (cond (= metric :day)   (visits-per-hour (specter/select [specter/ALL #(nxdt/same-day? (:time_local %) date)] ds))
        (= metric :week)  (visits-per-day (specter/select [specter/ALL #(nxdt/same-week? (:time_local %) date)] ds))
        (= metric :month) (visits-per-day (specter/select [specter/ALL #(nxdt/same-month? (:time_local %) date)] ds))
        (= metric :year)  (visits-per-month (specter/select [specter/ALL #(nxdt/same-year? (:time_local %) date)] ds))))

(defn overview
  [ds [from to] granularity]
  (let [interval-ds (specter/select
                     [specter/ALL
                      #((partial nxdt/within?
                                 (or from (nxdt/beginning))
                                 (or to (nxdt/now)))
                        (:time_local %))]
                     ds)]
    (cond (= granularity :hour)  (visits-per-hour interval-ds)
          (= granularity :day)   (visits-per-day  interval-ds)
          (= granularity :week)  (visits-per-day  interval-ds)
          (= granularity :month) (visits-per-month  interval-ds))))


;; TODO
;; = Pages =
;; 1. top pages

;;  = Referrals =
;; 1. top referrals

;;  = Technical =
;; 1. devices
;; 2. response code
;; 3. response time
