(ns nxstat.core
  (:require [nxstat.datetime :as nxdt]
            [nxstat.log :as log]
            [nxstat.stats :as stats]
            [incanter.core :as incanter]
            [incanter.charts :as icharts])
  (:gen-class))


(comment

  (def log-ds (log/load "nginx/"))

  "=== Traffic ==="

  "traffic in day as time-series-plot"
  (-> log-ds
      (stats/traffic "08/Mar/2016:23:37:02 +0000" :day)
      (incanter/with-data (icharts/time-series-plot :hour :visits :legend true :title "Traffic for 08/Mar/2016"))
      incanter/view)

  "traffic in a day as bar chart"
  (-> log-ds
      (stats/traffic "08/Mar/2016:23:37:02 +0000" :day)
      (incanter/with-data (icharts/bar-chart :hour :visits :title "Traffic for 08/Mar/2016"))
      incanter/view)

  "traffic in a week as time-series-plot"
  (-> log-ds
      (stats/traffic "08/Mar/2016:23:37:02 +0000" :week)
      (incanter/with-data (icharts/time-series-plot :day :visits))
      incanter/view)

  "traffic in a week as bar chart"
  (-> log-ds
      (stats/traffic "08/Apr/2016:23:37:02 +0000" :week)
      (incanter/$map )
      (incanter/with-data (icharts/bar-chart :day :visits))
      incanter/view)

  "traffic in a week as bar chart grouped by day"
  (-> log-ds
      (stats/traffic "08/Apr/2016:23:37:02 +0000" :week)
      (incanter/with-data (icharts/bar-chart :day :visits :legend true))
      incanter/view)

  "traffic in a month as time-series-plot"
  (-> log-ds
      (stats/traffic "18/Apr/2016:23:37:02 +0000" :month)
      (incanter/with-data (icharts/time-series-plot :day :visits))
      incanter/view)

  (-> log-ds
      (stats/traffic "18/Apr/2016:23:37:02 +0000" :month)
      (incanter/with-data (icharts/line-chart :day :visits :group-by :day))
      incanter/view)


  "traffic in a year as time-series-plot"
  (-> log-ds
      (stats/traffic "18/Apr/2016:23:37:02 +0000" :year)
      (incanter/with-data (icharts/time-series-plot :month :visits))
      incanter/view)

  "=== Overview ==="

  "traffic overview, granularity hour"
  (-> log-ds
      (stats/overview ["18/Mar/2016:23:37:02 +0000"] :hour)
      (incanter/with-data (icharts/time-series-plot :hour :visits))
      incanter/view)

  "traffic overview, granularity day"
  (-> log-ds
      (stats/overview ["18/Apr/2016:23:37:02 +0000"] :day)
      (incanter/with-data (icharts/time-series-plot :day :visits))
      incanter/view)

  "traffic overview, granularity week"
  (-> log-ds
      (stats/overview ["18/Apr/2016:23:37:02 +0000"] :week)
      (incanter/with-data (icharts/time-series-plot :day :visits))
      incanter/view)

  )
