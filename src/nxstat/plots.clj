(ns nxstat.plots
  (:require [nxstat.datetime :as nxdt]
            [nxstat.stats :as stats]
            [incanter.core :as incanter]
            [incanter.charts :as icharts]))

(defn- traffic-ts-plot
  [log-data date metric]
  (-> log-data
      (stats/traffic date metric)
      incanter/to-dataset
      (incanter/with-data (icharts/time-series-plot :time_local :count))
      incanter/view))

(defn- traffic-bar-chart
  [log-data date metric]
  (-> log-data
      (stats/traffic date metric)
      incanter/to-dataset
      (incanter/with-data (icharts/bar-chart :time_local :count :title "Traffic for 08/Mar/2016"))
      incanter/view))

(defn- overview-ts-plot
  [log-data interval granularity]
  (-> log-data
      (stats/overview interval granularity)
      incanter/to-dataset
      (incanter/with-data (icharts/time-series-plot :time_local :count))
      incanter/view))

(defn traffic-day
  "Traffic in a day as time-series-plot."
  [log-data date]
  (traffic-ts-plot log-data date :day))

(defn traffic-day-bar
  "Traffic in a day as bar chart."
  [log-data date]
  (traffic-bar-chart log-data date :day))

(defn traffic-week
  "Traffic in a week as time-series-plot."
  [log-data date]
  (traffic-ts-plot log-data date :week))

(defn traffic-week-bar
  "Traffic in a week as bar chart."
  [log-data date]
  (traffic-bar-chart log-data date :week))

(defn traffic-month
  "Traffic in a month as time-series-plot."
  [log-data date]
  (traffic-ts-plot log-data date :month))

(defn traffic-year
  "Traffic in a year as time-series-plot."
  [log-data date]
  (traffic-ts-plot log-data date :year))

  "=== Overview ==="

(defn overview-hour
  "Traffic overview, granularity hour."
  [log-data interval]
  (overview-ts-plot log-data interval :hour))

(defn overview-day
  "Traffic overview, granularity day."
  [log-data interval]
  (overview-ts-plot log-data interval :day))

(defn overview-week
  "Traffic overview, granularity week."
  [log-data interval]
  (overview-ts-plot log-data interval :week))
