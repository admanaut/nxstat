(ns nxstat.core
  (:require [nxstat.log :as log]
            [nxstat.plots :as plots])
  (:gen-class))


(comment

  (def log-data (log/load-access-logs "nginx/"))

  (plots/traffic-month log-data "08/Mar/2016:23:37:02 +0000")
  (plots/overview-hour log-data ["08/Mar/2016:23:37:02 +0000"])

  )
