(ns nxstat.core
  (:require [nxstat.datetime :as nxdt]
            [nxstat.log :as log]
            [nxstat.stats :as stats]
            [incanter.core :as incanter]
            [incanter.charts :as icharts]
            [com.rpl.specter :as specter])
  (:gen-class))


(comment

  (def log-data (log/load-access-logs "nginx/"))


  "=== Traffic ==="

  "traffic in a day as time-series-plot"
  (-> log-data
      (stats/traffic "08/Mar/2016:23:37:02 +0000" :day)
      incanter/to-dataset
      (incanter/with-data (icharts/time-series-plot :time_local :count :title "Traffic for 08/Mar/2016"))
      incanter/view)

  "traffic in a day as bar chart"
  (-> log-data
      (stats/traffic "08/Mar/2016:23:37:02 +0000" :day)
      incanter/to-dataset
      (incanter/with-data (icharts/bar-chart :time_local :count :title "Traffic for 08/Mar/2016"))
      incanter/view)

  "traffic in a week as time-series-plot"
  (-> log-data
      (stats/traffic "08/Mar/2016:23:37:02 +0000" :week)
      incanter/to-dataset
      (incanter/with-data (icharts/time-series-plot :time_local :count))
      incanter/view)

  "traffic in a week as bar chart"
  (-> log-data
      (stats/traffic "08/Apr/2016:23:37:02 +0000" :week)
      incanter/to-dataset
      (incanter/with-data (icharts/bar-chart :time_local :count))
      incanter/view)

  "traffic in a month as time-series-plot"
  (-> log-data
      (stats/traffic "18/Apr/2016:23:37:02 +0000" :month)
      incanter/to-dataset
      (incanter/with-data (icharts/time-series-plot :time_local :count))
      incanter/view)

  "traffic in a year as time-series-plot"
  (-> log-data
      (stats/traffic "18/Apr/2016:23:37:02 +0000" :year)
      incanter/to-dataset
      (incanter/with-data (icharts/time-series-plot :time_local :count))
      incanter/view)

  "=== Overview ==="

  "traffic overview, granularity hour"
  (-> log-data
      (stats/overview ["18/Mar/2016:23:37:02 +0000"] :hour)
      incanter/to-dataset
      (incanter/with-data (icharts/time-series-plot :time_local :count))
      incanter/view)

  "traffic overview, granularity day"
  (-> log-data
      (stats/overview ["18/Apr/2016:23:37:02 +0000"] :day)
      incanter/to-dataset
      (incanter/with-data (icharts/time-series-plot :time_local :count))
      incanter/view)

  "traffic overview, granularity week"
  (-> log-data
      (stats/overview ["18/Apr/2016:23:37:02 +0000"] :week)
      incanter/to-dataset
      (incanter/with-data (icharts/time-series-plot :time_local :count))
      incanter/view)

  )
